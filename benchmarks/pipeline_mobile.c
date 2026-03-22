#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <limits.h>

int get_core_frequency(int cpu_id);

#define FORCE_CPU_BINDING

#ifdef FORCE_CPU_BINDING
// Bind on Android using sched_setaffinity with thread TID
void bind_thread_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pid_t tid = (pid_t)syscall(SYS_gettid);
    int result = sched_setaffinity(tid, sizeof(cpu_set_t), &cpuset);
    if (result != 0) perror("sched_setaffinity");
    else printf("Thread %d bound to core %d\n", tid, core_id);
}

#define MAYBE_BIND(core) bind_thread_to_core(core)
#else
#define MAYBE_BIND(core)
#endif

typedef struct {
    double energies[8];
    double t;
} energy_data_t;

int get_energy_data(energy_data_t *ed) {
    char buf[4096];
    int fd = open("/sys/bus/iio/devices/iio:device0/energy_value", O_RDONLY);
    if (fd < 0) { perror("open energy sensor"); return -1; }
    int n = read(fd, buf, sizeof(buf) - 1);
    if (n < 0) { perror("read energy sensor"); close(fd); return -1; }
    buf[n] = '\0';
    close(fd);

    for (int i = 0; i < 8; i++) ed->energies[i] = 0.0;
    ed->t = 0.0;

    char *line = strtok(buf, "\n");
    while (line != NULL) {
        if (strncmp(line, "t=", 2) == 0) {
            double time_val;
            if (sscanf(line, "t=%lf", &time_val) == 1) ed->t = time_val;
        } else if (strncmp(line, "CH", 2) == 0) {
            int ch;
            if (sscanf(line, "CH%d", &ch) == 1) {
                char *comma = strchr(line, ',');
                if (comma) {
                    double val;
                    if (sscanf(comma + 1, " %lf", &val) == 1) {
                        if (ch >= 0 && ch < 8) ed->energies[ch] = val;
                    }
                }
            }
        }
        line = strtok(NULL, "\n");
    }
    return 0;
}

int get_core_frequency(int cpu_id) {
    char filename[128];
    snprintf(filename, sizeof(filename),
             "/sys/devices/system/cpu/cpu%d/cpufreq/scaling_cur_freq", cpu_id);
    FILE *fp = fopen(filename, "r");
    if (!fp) { perror("fopen frequency file"); return -1; }
    int freq;
    if (fscanf(fp, "%d", &freq) != 1) { fclose(fp); return -1; }
    fclose(fp);
    return freq;
}

#define NUM_CORES 8
#define INITIAL_MAX_SNAPSHOTS 1000

typedef struct {
    time_t timestamp;
    int freqs[NUM_CORES];
} snapshot_t;

snapshot_t *snapshots = NULL;
int snapshot_count = 0;
int snapshot_capacity = 0;

void *frequency_profiler(void *arg) {
    int *flag = (int *)arg;
    snapshot_capacity = INITIAL_MAX_SNAPSHOTS;
    snapshots = malloc(snapshot_capacity * sizeof(snapshot_t));
    if (!snapshots) { perror("malloc snapshots"); return NULL; }
    int interval = 1;
    while (*flag) {
        snapshot_t s;
        s.timestamp = time(NULL);
        for (int i = 0; i < NUM_CORES; i++) s.freqs[i] = get_core_frequency(i);
        if (snapshot_count >= snapshot_capacity) {
            int new_capacity = snapshot_capacity * 2;
            snapshot_t *new_snapshots = realloc(snapshots, new_capacity * sizeof(snapshot_t));
            if (!new_snapshots) { perror("realloc snapshots"); break; }
            snapshot_capacity = new_capacity; snapshots = new_snapshots;
        }
        snapshots[snapshot_count++] = s;
        sleep(interval);
    }
    return NULL;
}

void print_frequency_summary(void) {
    printf("\n--- FINAL FREQUENCY SUMMARY ---\n");
    for (int i = 0; i < NUM_CORES; i++) {
        int freq = get_core_frequency(i);
        if (freq != -1) printf("Core %d: %d kHz\n", i, freq);
        else printf("Core %d: read error\n", i);
    }
}

void print_all_frequency_snapshots(void) {
    printf("\n--- ALL FREQUENCY SNAPSHOTS ---\n");
    for (int i = 0; i < snapshot_count; i++) {
        printf("Snapshot %d (Timestamp: %ld):\n", i, snapshots[i].timestamp);
        for (int j = 0; j < NUM_CORES; j++) {
            if (snapshots[i].freqs[j] != -1) printf("  Core %d: %d kHz\n", j, snapshots[i].freqs[j]);
            else printf("  Core %d: read error\n", j);
        }
        printf("\n");
    }
}

// ------------------------------------------------------------------------
#define MAX_THREADS 9
int global_tid = 0;
int global_cpu_mapping[MAX_THREADS] = {0};

#define NUM_TASKS 10
#define TERMINATION_TASK -1

#define GROUP_1 1
#define GROUP_2 2
#define GROUP_3 3
#define GROUP_4 4

// Default per-group thread counts (can be overridden by CLI in odroid form)
int STAGE1_THREADS = 1;
int STAGE2_THREADS = 1;
int STAGE3_THREADS = 1;
int GROUP2_STAGE12_THREADS = 2;
int GROUP2_STAGE3_THREADS = 1;
int GROUP3_STAGE1_THREADS = 1;
int GROUP3_STAGE23_THREADS = 2;
int GROUP4_THREADS = 3;

int queue1_size = 4;
int queue2_size = 4;
int queue3_size = 4;

typedef struct {
    int *elements;
    int read_pos, write_pos, count;
    int size;       // configured bound size; <=0 => unbounded
    int capacity;   // current allocated capacity (for unbounded)
    int index;
    pthread_cond_t not_full, not_empty;
} queue_t;

pthread_mutex_t queue_mutex[4];

#define UNBOUNDED_INITIAL_CAPACITY 16

queue_t* init_queue(int size, int idx) {
    queue_t *q = malloc(sizeof(queue_t));
    if (!q) { perror("malloc queue_t failed"); exit(EXIT_FAILURE); }
    q->read_pos = q->write_pos = q->count = 0;
    q->size = size;
    q->index = idx;
    q->capacity = 0;
    q->elements = NULL;
    if (size > 0) {
        q->capacity = size;
        q->elements = malloc(q->capacity * sizeof(int));
        if (!q->elements) { perror("malloc queue elements failed"); exit(EXIT_FAILURE); }
    }
    pthread_cond_init(&q->not_full, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    return q;
}

void free_queue(queue_t *q) {
    if (q->elements) free(q->elements);
    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
    free(q);
}

static int ensure_capacity_for_write(queue_t *q) {
    if (q->size > 0) return 0;
    if (q->capacity == 0) {
        int newcap = UNBOUNDED_INITIAL_CAPACITY;
        q->elements = malloc(newcap * sizeof(int));
        if (!q->elements) return -1;
        q->capacity = newcap;
        q->read_pos = q->write_pos = q->count = 0;
        return 0;
    }
    if (q->count < q->capacity) return 0;
    int newcap = q->capacity * 2;
    int *newbuf = realloc(q->elements, newcap * sizeof(int));
    if (!newbuf) return -1;
    if (q->read_pos > q->write_pos) {
        int *tmp = malloc(q->count * sizeof(int));
        if (!tmp) { free(newbuf); return -1; }
        for (int i = 0; i < q->count; i++) tmp[i] = q->elements[(q->read_pos + i) % q->capacity];
        int *finalbuf = malloc(newcap * sizeof(int));
        if (!finalbuf) { free(tmp); free(newbuf); return -1; }
        memcpy(finalbuf, tmp, q->count * sizeof(int));
        free(tmp); free(newbuf);
        q->elements = finalbuf;
        q->capacity = newcap;
        q->read_pos = 0;
        q->write_pos = q->count % newcap;
        return 0;
    } else {
        q->elements = newbuf;
        q->capacity = newcap;
        return 0;
    }
}

void write_queue(queue_t *q, int value) {
    pthread_mutex_lock(&queue_mutex[q->index]);
    if (q->size > 0) {
        while (q->count == q->size) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            if (pthread_cond_timedwait(&q->not_full, &queue_mutex[q->index], &ts) == ETIMEDOUT) {
                printf("Queue %d is full. Backing off...\n", q->index + 1);
                pthread_mutex_unlock(&queue_mutex[q->index]);
                return;
            }
        }
        q->elements[q->write_pos] = value;
        q->write_pos = (q->write_pos + 1) % q->capacity;
        q->count++;
        pthread_cond_signal(&q->not_empty);
        pthread_mutex_unlock(&queue_mutex[q->index]);
        return;
    }
    if (ensure_capacity_for_write(q) != 0) {
        printf("Queue %d allocation failed. Backing off...\n", q->index + 1);
        pthread_mutex_unlock(&queue_mutex[q->index]);
        return;
    }
    q->elements[q->write_pos] = value;
    q->write_pos = (q->write_pos + 1) % q->capacity;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&queue_mutex[q->index]);
}

int read_queue(queue_t *q) {
    pthread_mutex_lock(&queue_mutex[q->index]);
    while (q->count == 0) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 14;
        if (pthread_cond_timedwait(&q->not_empty, &queue_mutex[q->index], &ts) == ETIMEDOUT) {
            printf("Queue %d is empty. Returning TERM_TOKEN...\n", q->index + 1);
            pthread_mutex_unlock(&queue_mutex[q->index]);
            return TERMINATION_TASK;
        }
    }
    int value = q->elements[q->read_pos];
    q->read_pos = (q->read_pos + 1) % q->capacity;
    q->count--;
    if (q->size > 0) pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&queue_mutex[q->index]);
    return value;
}

// Workload configuration parsed from CLI
char workload_type[32];
int cpu_iters_stage1, cpu_iters_stage2, cpu_iters_stage3;
size_t mem_size_stage1, mem_size_stage2, mem_size_stage3;
int pipeline_group;

// Workload implementations (use configured sizes)
void cpu_workload_iters(int iterations) {
    double sum = 1.0;
    for (int i = 1; i <= iterations; i++) {
        sum *= (i % 10 + 1.1) / 1.05;
        sum += (sum / (i % 5 + 1.2)) * 1.03;
        sum = sum - (sum / 1.07);
    }
    volatile double result = sum;
    (void)result;
}

void memory_workload_sz(size_t array_size) {
    int *array = malloc((array_size > 0 ? array_size : 1) * sizeof(int));
    if (!array) { perror("Memory allocation failed"); exit(EXIT_FAILURE); }
    for (size_t i = 0; i < array_size; i++) array[i] = (int)i;
    volatile int accessed_values = 0;
    size_t stride = 64;
    for (size_t i = 0; i < array_size; i += stride) accessed_values += array[i];
    printf("Memory workload accessed_values: %d\n", accessed_values);
    fflush(stdout);
    free(array);
}

void run_stage_workload(int stage) {
    if (strcmp(workload_type, "cpu_only") == 0) {
        cpu_workload_iters((stage==1)?cpu_iters_stage1:(stage==2?cpu_iters_stage2:cpu_iters_stage3));
    } else if (strcmp(workload_type, "memory_only") == 0) {
        memory_workload_sz((stage==1)?mem_size_stage1:(stage==2?mem_size_stage2:mem_size_stage3));
    } else if (strcmp(workload_type, "cpu_and_memory") == 0) {
        if (stage == 1) cpu_workload_iters(cpu_iters_stage1);
        else if (stage == 2) memory_workload_sz(mem_size_stage2);
        else cpu_workload_iters(cpu_iters_stage3);
    } else if (strcmp(workload_type, "memory_and_cpu") == 0) {
        if (stage == 1) memory_workload_sz(mem_size_stage1);
        else if (stage == 2) cpu_workload_iters(cpu_iters_stage2);
        else memory_workload_sz(mem_size_stage3);
    }
}

typedef struct {
    int index;
    queue_t *in_q;
    queue_t *out_q;
    const char *stage_name;
    int bind_core;
    int stage3_variant;
} thread_param_t;

// Stage function implementations
void *stage1(void *arg) {
    thread_param_t *param = (thread_param_t *)arg;
    MAYBE_BIND(param->bind_core);
    int idx = param->index;
    while (1) {
        int task = read_queue(param->in_q);
        if (task == TERMINATION_TASK) break;
        run_stage_workload(1);
        if (param->out_q) write_queue(param->out_q, task);
        printf("Stage 1 - Global Thread %d processed input %d\n", idx, task);
        fflush(stdout);
    }
    return NULL;
}

void *stage2(void *arg) {
    thread_param_t *param = (thread_param_t *)arg;
    MAYBE_BIND(param->bind_core);
    int idx = param->index;
    while (1) {
        int task = read_queue(param->in_q);
        if (task == TERMINATION_TASK) break;
        run_stage_workload(2);
        if (param->out_q) write_queue(param->out_q, task);
        printf("Stage 2 - Global Thread %d processed input %d\n", idx, task);
        fflush(stdout);
    }
    return NULL;
}

void *stage3(void *arg) {
    thread_param_t *param = (thread_param_t *)arg;
    MAYBE_BIND(param->bind_core);
    int idx = param->index;
    while (1) {
        int task = read_queue(param->in_q);
        if (task == TERMINATION_TASK) break;
        run_stage_workload(3);
        printf("Stage 3 - Global Thread %d processed input %d\n", idx, task);
        fflush(stdout);
    }
    return NULL;
}

void *stage1_and_stage2(void *arg) {
    thread_param_t *param = (thread_param_t *)arg;
    MAYBE_BIND(param->bind_core);
    int idx = param->index;
    while (1) {
        int task = read_queue(param->in_q);
        if (task == TERMINATION_TASK) break;
        run_stage_workload(1);
        run_stage_workload(2);
        if (param->out_q) write_queue(param->out_q, task);
        printf("Stage 1+2 - Global Thread %d processed input %d\n", idx, task);
        fflush(stdout);
    }
    return NULL;
}

void *stage2_and_stage3(void *arg) {
    thread_param_t *param = (thread_param_t *)arg;
    MAYBE_BIND(param->bind_core);
    int idx = param->index;
    while (1) {
        int task = read_queue(param->in_q);
        if (task == TERMINATION_TASK) break;
        run_stage_workload(2);
        run_stage_workload(3);
        printf("Stage 2+3 - Global Thread %d processed input %d\n", idx, task);
        fflush(stdout);
    }
    return NULL;
}

void *merged_all_stages(void *arg) {
    thread_param_t *param = (thread_param_t *)arg;
    MAYBE_BIND(param->bind_core);
    int idx = param->index;
    while (1) {
        int task = read_queue(param->in_q);
        if (task == TERMINATION_TASK) break;
        run_stage_workload(1);
        run_stage_workload(2);
        run_stage_workload(3);
        printf("Stage 1+2+3 - Global Thread %d processed input %d\n", idx, task);
        fflush(stdout);
    }
    return NULL;
}

// ------------------------------------------------------------------------
int main(int argc, char *argv[]) {
#ifdef FORCE_CPU_BINDING
    // No online-core parsing in mobile version; rely on simple round-robin binding
    // Optionally user may bind by setting bind_core values below (we will use global_tid % NUM_CORES).
#endif

    // Start energy + frequency profiling
    energy_data_t energy_start, energy_end;
    struct timespec ts_start, ts_end;
    if (get_energy_data(&energy_start) != 0) fprintf(stderr, "Failed to read initial energy measurement\n");
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    volatile int profiling_flag = 1;
    pthread_t profiler_thread;
    if (pthread_create(&profiler_thread, NULL, frequency_profiler, (void *)&profiling_flag) != 0) perror("Failed to create frequency profiler thread");

    setvbuf(stdout, NULL, _IONBF, 0);

    // CLI: accept odroid-style and mobile original
    // odroid-style:
    //  ./pipe_3stages <workload_type> <s1> <s2> <s3> [ grouping queue... threads... ] (full)
    // short odroid: ./pipe_3stages <workload_type> <s1> <s2> <s3>  (expands to group1, queues unbounded, threads=1)
    if (argc == 5) {
        // short form
        strcpy(workload_type, argv[1]);
        cpu_iters_stage1 = atoi(argv[2]);
        cpu_iters_stage2 = atoi(argv[3]);
        cpu_iters_stage3 = atoi(argv[4]);
        mem_size_stage1 = (size_t)atoi(argv[2]);
        mem_size_stage2 = (size_t)atoi(argv[3]);
        mem_size_stage3 = (size_t)atoi(argv[4]);

        pipeline_group = GROUP_1;
        queue1_size = 0; queue2_size = 0; queue3_size = 0;
        STAGE1_THREADS = 1; STAGE2_THREADS = 1; STAGE3_THREADS = 1;

        printf("Short form detected. Expanded to canonical invocation:\n");
        printf("  %s %d %d %d grouping=1 q1=0 q2=0 q3=0 t1=1 t2=1 t3=1\n",
               workload_type, cpu_iters_stage1, cpu_iters_stage2, cpu_iters_stage3);
    } else {
        if (argc < 8) {
            fprintf(stderr, "Usage (short): %s <workload_type> <size1> <size2> <size3>\n", argv[0]);
            fprintf(stderr, "Usage (full):  %s <workload_type> <size1> <size2> <size3> <grouping> <queue...> <threads...>\n", argv[0]);
            exit(EXIT_FAILURE);
        }
        strcpy(workload_type, argv[1]);
        cpu_iters_stage1 = atoi(argv[2]);
        cpu_iters_stage2 = atoi(argv[3]);
        cpu_iters_stage3 = atoi(argv[4]);
        mem_size_stage1 = (size_t)atoi(argv[2]);
        mem_size_stage2 = (size_t)atoi(argv[3]);
        mem_size_stage3 = (size_t)atoi(argv[4]);

        pipeline_group = atoi(argv[5]);
        int thread_arg_index = 0;

        if (pipeline_group == GROUP_1 && argc >= 12) {
            queue1_size = atoi(argv[6]);
            queue2_size = atoi(argv[7]);
            queue3_size = atoi(argv[8]);
            thread_arg_index = 9;
            STAGE1_THREADS = atoi(argv[thread_arg_index]);
            STAGE2_THREADS = atoi(argv[thread_arg_index + 1]);
            STAGE3_THREADS = atoi(argv[thread_arg_index + 2]);
        } else if (pipeline_group == GROUP_2 && argc >= 10) {
            queue1_size = atoi(argv[6]);
            queue3_size = atoi(argv[7]);
            thread_arg_index = 8;
            GROUP2_STAGE12_THREADS = atoi(argv[thread_arg_index]);
            GROUP2_STAGE3_THREADS = atoi(argv[thread_arg_index + 1]);
        } else if (pipeline_group == GROUP_3 && argc >= 10) {
            queue1_size = atoi(argv[6]);
            queue2_size = atoi(argv[7]);
            thread_arg_index = 8;
            GROUP3_STAGE1_THREADS = atoi(argv[thread_arg_index]);
            GROUP3_STAGE23_THREADS = atoi(argv[thread_arg_index + 1]);
        } else if (pipeline_group == GROUP_4 && argc >= 8) {
            queue1_size = atoi(argv[6]);
            thread_arg_index = 7;
            GROUP4_THREADS = atoi(argv[thread_arg_index]);
        } else {
            fprintf(stderr, "Invalid arguments for the selected pipeline group.\n");
            exit(EXIT_FAILURE);
        }
    }

    // Dispatch pipeline groups (keeping the original mobile program structure/prints)
    if (pipeline_group == GROUP_1) {
        printf("Running pipeline group: 1 (Separate Stage1, Stage2, Stage3)\n");
        queue_t *queue1 = init_queue(queue1_size, 0);
        queue_t *queue2 = init_queue(queue2_size, 1);
        queue_t *queue3 = init_queue(queue3_size, 2);
        for (int i = 0; i < 3; i++) pthread_mutex_init(&queue_mutex[i], NULL);

        pthread_t s1_threads[STAGE1_THREADS];
        pthread_t s2_threads[STAGE2_THREADS];
        pthread_t s3_threads[STAGE3_THREADS];
        thread_param_t stage1_params[STAGE1_THREADS];
        thread_param_t stage2_params[STAGE2_THREADS];
        thread_param_t stage3_params[STAGE3_THREADS];

        for (int i = 0; i < STAGE1_THREADS; i++) {
            stage1_params[i].index = global_tid;
            stage1_params[i].in_q = queue1;
            stage1_params[i].out_q = queue2;
            stage1_params[i].stage_name = "Group1 - Stage 1";
            stage1_params[i].bind_core = (global_tid % NUM_CORES);
            global_cpu_mapping[global_tid] = stage1_params[i].bind_core;
            global_tid++;
            pthread_create(&s1_threads[i], NULL, stage1, &stage1_params[i]);
        }
        for (int i = 0; i < STAGE2_THREADS; i++) {
            stage2_params[i].index = global_tid;
            stage2_params[i].in_q = queue2;
            stage2_params[i].out_q = queue3;
            stage2_params[i].stage_name = "Group1 - Stage 2";
            stage2_params[i].bind_core = (global_tid % NUM_CORES);
            global_cpu_mapping[global_tid] = stage2_params[i].bind_core;
            global_tid++;
            pthread_create(&s2_threads[i], NULL, stage2, &stage2_params[i]);
        }
        for (int i = 0; i < STAGE3_THREADS; i++) {
            stage3_params[i].index = global_tid;
            stage3_params[i].in_q = queue3;
            stage3_params[i].out_q = NULL;
            stage3_params[i].stage_name = "Group1 - Stage 3";
            stage3_params[i].bind_core = (global_tid % NUM_CORES);
            stage3_params[i].stage3_variant = 0;
            global_cpu_mapping[global_tid] = stage3_params[i].bind_core;
            global_tid++;
            pthread_create(&s3_threads[i], NULL, stage3, &stage3_params[i]);
        }

        int t = 0;
        while (t < NUM_TASKS) {
            int free_slots = (queue1->size > 0) ? (queue1->size - queue1->count) : (INT_MAX);
            if (free_slots > 0) {
                int batch_size = (free_slots < (NUM_TASKS - t)) ? free_slots : (NUM_TASKS - t);
                for (int j = 0; j < batch_size; j++, t++) write_queue(queue1, t);
            } else sched_yield();
        }

        for (int j = 0; j < STAGE1_THREADS; j++) write_queue(queue1, TERMINATION_TASK);
        for (int j = 0; j < STAGE1_THREADS; j++) pthread_join(s1_threads[j], NULL);
        for (int j = 0; j < STAGE2_THREADS; j++) write_queue(queue2, TERMINATION_TASK);
        for (int j = 0; j < STAGE2_THREADS; j++) pthread_join(s2_threads[j], NULL);
        for (int j = 0; j < STAGE3_THREADS; j++) write_queue(queue3, TERMINATION_TASK);
        for (int j = 0; j < STAGE3_THREADS; j++) pthread_join(s3_threads[j], NULL);

        for (int i = 0; i < 3; i++) pthread_mutex_destroy(&queue_mutex[i]);
        free_queue(queue1); free_queue(queue2); free_queue(queue3);

        printf("\n=== Debugging Summary ===\n");
        printf("Group: 1\n");
        printf("Total threads: %d\n", global_tid);
        printf("Stage 1 threads: %d\n", STAGE1_THREADS);
        printf("Stage 2 threads: %d\n", STAGE2_THREADS);
        printf("Stage 3 threads: %d\n", STAGE3_THREADS);
        printf("Queue 1 Size: %d\n", queue1_size);
        printf("Queue 2 Size: %d\n", queue2_size);
        printf("Queue 3 Size: %d\n", queue3_size);
    }
    else if (pipeline_group == GROUP_2) {
        printf("Running pipeline group: 2 (Merged Stage1+2, Separate Stage3)\n");
        queue_t *queue1 = init_queue(queue1_size, 0);
        queue_t *queue3 = init_queue(queue3_size, 1);
        for (int i = 0; i < 2; i++) pthread_mutex_init(&queue_mutex[i], NULL);

        pthread_t st12_threads[GROUP2_STAGE12_THREADS];
        pthread_t s3_threads[GROUP2_STAGE3_THREADS];
        thread_param_t group2_st12_params[GROUP2_STAGE12_THREADS];
        thread_param_t group2_s3_params[GROUP2_STAGE3_THREADS];

        for (int i = 0; i < GROUP2_STAGE12_THREADS; i++) {
            group2_st12_params[i].index = global_tid;
            group2_st12_params[i].in_q = queue1;
            group2_st12_params[i].out_q = queue3;
            group2_st12_params[i].stage_name = "Group2 - Merged Stage1+2";
            group2_st12_params[i].bind_core = (global_tid % NUM_CORES);
            global_cpu_mapping[global_tid] = group2_st12_params[i].bind_core;
            global_tid++;
            pthread_create(&st12_threads[i], NULL, stage1_and_stage2, &group2_st12_params[i]);
        }
        for (int i = 0; i < GROUP2_STAGE3_THREADS; i++) {
            group2_s3_params[i].index = global_tid;
            group2_s3_params[i].in_q = queue3;
            group2_s3_params[i].out_q = NULL;
            group2_s3_params[i].stage_name = "Group2 - Stage 3";
            group2_s3_params[i].bind_core = (global_tid % NUM_CORES);
            group2_s3_params[i].stage3_variant = 1;
            global_cpu_mapping[global_tid] = group2_s3_params[i].bind_core;
            global_tid++;
            pthread_create(&s3_threads[i], NULL, stage3, &group2_s3_params[i]);
        }

        int t = 0;
        while (t < NUM_TASKS) {
            int free_slots = (queue1->size > 0) ? (queue1->size - queue1->count) : (INT_MAX);
            if (free_slots > 0) {
                int batch_size = (free_slots < (NUM_TASKS - t)) ? free_slots : (NUM_TASKS - t);
                for (int j = 0; j < batch_size; j++, t++) write_queue(queue1, t);
            } else sched_yield();
        }

        for (int j = 0; j < GROUP2_STAGE12_THREADS; j++) write_queue(queue1, TERMINATION_TASK);
        for (int j = 0; j < GROUP2_STAGE12_THREADS; j++) pthread_join(st12_threads[j], NULL);
        for (int j = 0; j < GROUP2_STAGE3_THREADS; j++) write_queue(queue3, TERMINATION_TASK);
        for (int j = 0; j < GROUP2_STAGE3_THREADS; j++) pthread_join(s3_threads[j], NULL);

        for (int i = 0; i < 2; i++) pthread_mutex_destroy(&queue_mutex[i]);
        free_queue(queue1); free_queue(queue3);

        printf("\n=== Debugging Summary ===\n");
        printf("Group: 2\n");
        printf("Total threads: %d\n", global_tid);
        printf("Merged Stage1+2 threads: %d\n", GROUP2_STAGE12_THREADS);
        printf("Stage 3 threads: %d\n", GROUP2_STAGE3_THREADS);
        printf("Queue 1 Size: %d\n", queue1_size);
        printf("Queue 3 Size: %d\n", queue3_size);
    }
    else if (pipeline_group == GROUP_3) {
        printf("Running pipeline group: 3 (Separate Stage1, Merged Stage2+3)\n");
        queue_t *queue1 = init_queue(queue1_size, 0);
        queue_t *queue2 = init_queue(queue2_size, 1);
        for (int i = 0; i < 2; i++) pthread_mutex_init(&queue_mutex[i], NULL);

        pthread_t s1_threads[GROUP3_STAGE1_THREADS];
        pthread_t st23_threads[GROUP3_STAGE23_THREADS];
        thread_param_t group3_s1_params[GROUP3_STAGE1_THREADS];
        thread_param_t group3_st23_params[GROUP3_STAGE23_THREADS];

        for (int i = 0; i < GROUP3_STAGE1_THREADS; i++) {
            group3_s1_params[i].index = global_tid;
            group3_s1_params[i].in_q = queue1;
            group3_s1_params[i].out_q = queue2;
            group3_s1_params[i].stage_name = "Group3 - Stage 1";
            group3_s1_params[i].bind_core = (global_tid % NUM_CORES);
            global_cpu_mapping[global_tid] = group3_s1_params[i].bind_core;
            global_tid++;
            pthread_create(&s1_threads[i], NULL, stage1, &group3_s1_params[i]);
        }
        for (int i = 0; i < GROUP3_STAGE23_THREADS; i++) {
            group3_st23_params[i].index = global_tid;
            group3_st23_params[i].in_q = queue2;
            group3_st23_params[i].out_q = NULL;
            group3_st23_params[i].stage_name = "Group3 - Merged Stage2+3";
            group3_st23_params[i].bind_core = (global_tid % NUM_CORES);
            global_cpu_mapping[global_tid] = group3_st23_params[i].bind_core;
            global_tid++;
            pthread_create(&st23_threads[i], NULL, stage2_and_stage3, &group3_st23_params[i]);
        }

        int t = 0;
        while (t < NUM_TASKS) {
            int free_slots = (queue1->size > 0) ? (queue1->size - queue1->count) : (INT_MAX);
            if (free_slots > 0) {
                int batch_size = (free_slots < (NUM_TASKS - t)) ? free_slots : (NUM_TASKS - t);
                for (int j = 0; j < batch_size; j++, t++) write_queue(queue1, t);
            } else sched_yield();
        }

        for (int j = 0; j < GROUP3_STAGE1_THREADS; j++) write_queue(queue1, TERMINATION_TASK);
        for (int j = 0; j < GROUP3_STAGE1_THREADS; j++) pthread_join(s1_threads[j], NULL);
        for (int j = 0; j < GROUP3_STAGE23_THREADS; j++) write_queue(queue2, TERMINATION_TASK);
        for (int j = 0; j < GROUP3_STAGE23_THREADS; j++) pthread_join(st23_threads[j], NULL);

        for (int i = 0; i < 2; i++) pthread_mutex_destroy(&queue_mutex[i]);
        free_queue(queue1); free_queue(queue2);

        printf("\n=== Debugging Summary ===\n");
        printf("Group: 3\n");
        printf("Total threads: %d\n", global_tid);
        printf("Stage 1 threads: %d\n", GROUP3_STAGE1_THREADS);
        printf("Merged Stage2+3 threads: %d\n", GROUP3_STAGE23_THREADS);
        printf("Queue 1 Size: %d\n", queue1_size);
        printf("Queue 2 Size: %d\n", queue2_size);
    }
    else if (pipeline_group == GROUP_4) {
        printf("Running pipeline group: 4 (Merged All-Stages)\n");
        queue_t *queue1 = init_queue(queue1_size, 0);
        pthread_mutex_init(&queue_mutex[0], NULL);
        pthread_t merged_threads[GROUP4_THREADS];
        thread_param_t group4_params[GROUP4_THREADS];

        for (int i = 0; i < GROUP4_THREADS; i++) {
            group4_params[i].index = global_tid;
            group4_params[i].in_q = queue1;
            group4_params[i].out_q = NULL;
            group4_params[i].stage_name = "Group4 - Merged All-Stages";
            group4_params[i].bind_core = (global_tid % NUM_CORES);
            global_cpu_mapping[global_tid] = group4_params[i].bind_core;
            global_tid++;
            pthread_create(&merged_threads[i], NULL, merged_all_stages, &group4_params[i]);
        }

        int t = 0;
        while (t < NUM_TASKS) {
            int free_slots = (queue1->size > 0) ? (queue1->size - queue1->count) : (INT_MAX);
            if (free_slots > 0) {
                int batch_size = (free_slots < (NUM_TASKS - t)) ? free_slots : (NUM_TASKS - t);
                for (int j = 0; j < batch_size; j++, t++) write_queue(queue1, t);
            } else sched_yield();
        }

        for (int j = 0; j < GROUP4_THREADS; j++) write_queue(queue1, TERMINATION_TASK);
        for (int j = 0; j < GROUP4_THREADS; j++) pthread_join(merged_threads[j], NULL);

        pthread_mutex_destroy(&queue_mutex[0]);
        free_queue(queue1);

        printf("\n=== Debugging Summary ===\n");
        printf("Group: 4\n");
        printf("Total threads: %d\n", global_tid);
        printf("Merged All-Stages threads: %d\n", GROUP4_THREADS);
        printf("Queue 1 Size: %d\n", queue1_size);
    }
    else {
        fprintf(stderr, "Invalid pipeline group specified.\n");
        exit(EXIT_FAILURE);
    }

    printf("\n=== Workload Summary ===\n");
    printf("Workload Type: %s\n", workload_type);
    printf("Stage 1 Size: %d\n", cpu_iters_stage1);
    printf("Stage 2 Size: %d\n", cpu_iters_stage2);
    printf("Stage 3 Size: %d\n", cpu_iters_stage3);
    printf("Group2 Threads: %d %d\n", GROUP2_STAGE12_THREADS, GROUP2_STAGE3_THREADS);
    printf("Group3 Threads: %d %d\n", GROUP3_STAGE1_THREADS, GROUP3_STAGE23_THREADS);
    printf("Group4 Threads: %d\n", GROUP4_THREADS);
    printf("========================\n\n");

    printf("CPU Mapping (Thread -> Core):\n");
    for (int i = 0; i < global_tid; i++) {
        printf("  Thread %d -> Core %d\n", i, global_cpu_mapping[i]);
    }
    printf("==========================\n\n");

    profiling_flag = 0;
    pthread_join(profiler_thread, NULL);

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    if (get_energy_data(&energy_end) != 0) fprintf(stderr, "Failed to read final energy measurement\n");
    double energy_rails_CPUs_only = ((energy_end.energies[3] - energy_start.energies[3]) +
         (energy_end.energies[4] - energy_start.energies[4]) +
         (energy_end.energies[5] - energy_start.energies[5])) / 1000000.0;
    double energy_rails_all = 0.0;
    for (int i = 0; i < 8; i++) energy_rails_all += (energy_end.energies[i] - energy_start.energies[i]);
    energy_rails_all /= 1000000.0;
    double time_elapsed = (ts_end.tv_sec - ts_start.tv_sec) + (ts_end.tv_nsec - ts_start.tv_nsec) / 1e9;
    double power_rails_CPUs_only = energy_rails_CPUs_only / time_elapsed;
    double power_rails_all = energy_rails_all / time_elapsed;

    print_frequency_summary();
    print_all_frequency_snapshots();
    free(snapshots);

    printf("\n--- ENERGY MEASUREMENT SUMMARY ---\n");
    printf("Elapsed Time: %.2f seconds\n", time_elapsed);
    printf("Energy (CPUs only - Channels 3+4+5): %.2f Joules\n", energy_rails_CPUs_only);
    printf("Energy (All channels): %.2f Joules\n", energy_rails_all);
    printf("Power (CPUs only): %.2f Watts\n", power_rails_CPUs_only);
    printf("Power (All channels): %.2f Watts\n", power_rails_all);

    return 0;
}
