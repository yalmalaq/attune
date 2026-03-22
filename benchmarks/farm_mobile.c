#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sched.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <time.h>

int get_core_frequency(int cpu_id);

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
    while (line) {
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
            snapshot_capacity = new_capacity;
            snapshots = new_snapshots;
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
#define INPUT_BUFFER_SIZE 100
#define OUTPUT_BUFFER_SIZE 100
#define DEBUG_INTERVAL 10
#define MAX_WORKERS 32

int chunk_size = 2;
int num_workers = 0;

int input_buffer[INPUT_BUFFER_SIZE];
int output_buffer[OUTPUT_BUFFER_SIZE];
int input_index = 0, output_index = 0;

pthread_mutex_t input_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t output_mutex = PTHREAD_MUTEX_INITIALIZER;

// ------------------------------------------------------------------------
// Workloads exactly as in farm_odroid
void cpu_workload(size_t loops) {
    double sum = 1.0;
    for (size_t i = 1; i <= loops; i++) {
        sum *= (i % 10 + 1.1) / 1.05;
        sum += (sum / (i % 5 + 1.2)) * 1.03;
        sum -= (sum / 1.07);
    }
    volatile double result = sum;
    (void)result;
}

void memory_workload(size_t sz) {
    int *arr = malloc((sz > 0 ? sz : 1) * sizeof(int));
    if (!arr) exit(EXIT_FAILURE);
    for (size_t i = 0; i < sz; i++) arr[i] = (int)i;
    volatile int s = 0;
    for (size_t i = 0; i < sz; i += (64 > 1 ? 64 : 1)) s += arr[i];
    printf("Memory workload accessed_values: %d\n", s);
    free(arr);
    (void)s;
}

// Per-worker configuration (used only when odroid-style parsing is active)
typedef enum { PW_UNSET = 0, PW_CPU, PW_MEM } per_worker_e;
static per_worker_e per_worker_kind[MAX_WORKERS];
static size_t per_worker_size[MAX_WORKERS];

// ------------------------------------------------------------------------
typedef void (*workload_func_t)(void);

typedef struct {
    int thread_count;
    workload_func_t workload; // kept for compatibility with prints; often NULL when odroid-style used
} worker_config_t;

worker_config_t *worker_configs = NULL;

typedef struct {
    int start_index;
    int end_index;
    int *work_chunk;
    workload_func_t workload;
    int sub_index;
    int worker_id;
} sub_work_t;

// Helper: execute the configured workload for a worker id once
static inline void run_configured_workload(int worker_id) {
    if (worker_id < 0 || worker_id >= MAX_WORKERS) {
        cpu_workload(1000000);
        return;
    }
    if (per_worker_kind[worker_id] == PW_CPU) cpu_workload(per_worker_size[worker_id]);
    else if (per_worker_kind[worker_id] == PW_MEM) memory_workload(per_worker_size[worker_id]);
    else cpu_workload(1000000);
}

// Single sub_worker implementation used by all worker threads
void *sub_worker(void *arg) {
    sub_work_t *data = (sub_work_t *)arg;
    if (!data) return NULL;

    printf("[Subworker] (Worker %d, sub %d) processing indices %d to %d.\n",
           data->worker_id, data->sub_index, data->start_index, data->end_index);
    fflush(stdout);

    for (int i = data->start_index; i < data->end_index; i++) {
        if (data->workload) data->workload();
        else run_configured_workload(data->worker_id);
    }

    free(data);
    return NULL;
}

// Worker thread function (partitions and spawns subworkers)
void *worker(void *arg) {
    int worker_id = *(int *)arg;
    worker_config_t config = worker_configs[worker_id];
    int thread_count = config.thread_count;
    workload_func_t my_workload = config.workload; // may be NULL
    int chunk_counter = 0;

    printf("Worker %d: Using %d thread(s) with workload function at %p.\n",
           worker_id, thread_count, (void *)my_workload);
    fflush(stdout);

    while (1) {
        int local_chunk[chunk_size > 0 ? chunk_size : INPUT_BUFFER_SIZE];
        int chunk_count = 0;

        pthread_mutex_lock(&input_mutex);
        if (chunk_size == 0) {
            while (input_index < INPUT_BUFFER_SIZE && chunk_count < INPUT_BUFFER_SIZE)
                local_chunk[chunk_count++] = input_buffer[input_index++];
        } else {
            for (int i = 0; i < chunk_size && input_index < INPUT_BUFFER_SIZE; i++) {
                local_chunk[i] = input_buffer[input_index++];
                chunk_count++;
            }
        }
        pthread_mutex_unlock(&input_mutex);

        chunk_counter++;

        if (chunk_count == 0) {
            printf("Worker %d: No more work. Processed %d chunks. Exiting.\n", worker_id, chunk_counter);
            fflush(stdout);
            break;
        }

        int *partition_sizes = malloc(thread_count * sizeof(int));
        if (!partition_sizes) { perror("malloc partition_sizes"); break; }
        int tasks_per_partition = chunk_count / thread_count;
        int remainder = chunk_count % thread_count;
        for (int p = 0; p < thread_count; p++) partition_sizes[p] = tasks_per_partition + (p < remainder ? 1 : 0);

        int partition_start = 0;
        int tasks0 = partition_sizes[0];

        // main thread runs its partition
        for (int i = partition_start; i < partition_start + tasks0; i++) {
            if (my_workload) my_workload();
            else run_configured_workload(worker_id);
        }
        partition_start += tasks0;

        pthread_t *sub_threads = NULL;
        if (thread_count > 1) sub_threads = malloc((thread_count - 1) * sizeof(pthread_t));
        for (int p = 1; p < thread_count; p++) {
            sub_work_t *work = malloc(sizeof(sub_work_t));
            if (!work) { perror("malloc sub_work"); continue; }
            work->start_index = partition_start;
            work->end_index = partition_start + partition_sizes[p];
            work->work_chunk = local_chunk;
            work->workload = my_workload; // if NULL, sub_worker calls run_configured_workload
            work->sub_index = p;
            work->worker_id = worker_id;
            pthread_create(&sub_threads[p - 1], NULL, sub_worker, work);
            partition_start += partition_sizes[p];
        }

        for (int p = 0; p < thread_count - 1; p++) pthread_join(sub_threads[p], NULL);
        free(sub_threads);
        free(partition_sizes);

        pthread_mutex_lock(&output_mutex);
        for (int i = 0; i < chunk_count && output_index < OUTPUT_BUFFER_SIZE; i++) output_buffer[output_index++] = local_chunk[i];
        pthread_mutex_unlock(&output_mutex);

        if ((chunk_counter % DEBUG_INTERVAL) == 0) {
            printf("Worker %d: Processed %d chunks so far. Last chunk had %d tasks.\n",
                   worker_id, chunk_counter, chunk_count);
            fflush(stdout);
        }
    }

    printf("Worker %d: Exiting.\n", worker_id);
    fflush(stdout);
    return NULL;
}

// ------------------------------------------------------------------------

#ifdef USE_CUSTOM_AFFINITY
void bind_thread_to_core_by_id(pthread_t thread, int thread_id, int *core_mapping, int mapping_size) {
    int chosen_core = core_mapping[thread_id % mapping_size];
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(chosen_core, &cpuset);
    pid_t tid = (pid_t)syscall(SYS_gettid);
    int result = sched_setaffinity(tid, sizeof(cpu_set_t), &cpuset);
    if (result != 0) perror("sched_setaffinity");
    else printf("TThread %d bound to core %d (custom binding).\n", tid, chosen_core);
    fflush(stdout);
}
#endif

// ------------------------------------------------------------------------

int main(int argc, char *argv[]) {
    energy_data_t energy_start, energy_end;
    struct timespec ts_start, ts_end;
    if (get_energy_data(&energy_start) != 0) fprintf(stderr, "Failed to read initial energy measurement\n");
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    volatile int profiling_flag = 1;
    pthread_t profiler_thread;
    if (pthread_create(&profiler_thread, NULL, frequency_profiler, (void *)&profiling_flag) != 0)
        perror("Failed to create frequency profiler thread");

    setvbuf(stdout, NULL, _IONBF, 0);

    // CLI: support original farm_mobile and odroid-style short & full
    int parsed_odroid = 0;
    char *odroid_mode = NULL;
    size_t odroid_sizes[MAX_WORKERS] = {0};
    int odroid_threads[MAX_WORKERS] = {0};

    if (argc >= 3) {
        if (!strcmp(argv[2], "cpu_only") || !strcmp(argv[2], "memory_only") ||
            !strcmp(argv[2], "cpu_and_memory") || !strcmp(argv[2], "memory_and_cpu")) {
            parsed_odroid = 1;
            odroid_mode = argv[2];
            num_workers = atoi(argv[1]);
            if (num_workers <= 0 || num_workers > MAX_WORKERS) { fprintf(stderr, "num_workers out of range\n"); return 1; }
            int expected_short = 3 + num_workers;
            int expected_full_min = 4 + 2 * num_workers;
            if (argc == expected_short) {
                // short form: chunk_size = 0, threads = 1 each
                for (int i = 0; i < num_workers; ++i) { odroid_sizes[i] = strtoull(argv[3 + i], NULL, 0); odroid_threads[i] = 1; }
                chunk_size = 0;
            } else if (argc >= expected_full_min) {
                // full form: sizes, chunk, threads
                for (int i = 0; i < num_workers; ++i) odroid_sizes[i] = strtoull(argv[3 + i], NULL, 0);
                chunk_size = atoi(argv[3 + num_workers]);
                for (int i = 0; i < num_workers; ++i) { odroid_threads[i] = atoi(argv[4 + num_workers + i]); if (odroid_threads[i] <= 0) odroid_threads[i] = 1; }
            } else { fprintf(stderr, "Invalid argument count for %d workers\n", num_workers); return 1; }
        }
    }

    if (!parsed_odroid) {
        // original farm_mobile CLI
        if (argc < 3) {
            fprintf(stderr, "Usage: %s <chunk_size> <num_workers> <threads for worker0> ... <threads for workerN-1>\n", argv[0]);
            return 1;
        }
        chunk_size = atoi(argv[1]);
        if (chunk_size <= 0) { fprintf(stderr, "Invalid chunk_size.\n"); return 1; }
        num_workers = atoi(argv[2]);
        if (num_workers <= 0) { fprintf(stderr, "Invalid num_workers.\n"); return 1; }
        if (argc != (num_workers + 3)) {
            fprintf(stderr, "Error: Expected %d thread values (one per worker), but got %d.\n", num_workers, argc - 3);
            fprintf(stderr, "Usage: %s <chunk_size> <num_workers> <threads for worker0> ... <threads for workerN-1>\n", argv[0]);
            return 1;
        }
    }

    worker_configs = malloc(num_workers * sizeof(worker_config_t));
    if (!worker_configs) { perror("malloc failed for worker_configs"); return 1; }

    if (parsed_odroid) {
        for (int i = 0; i < num_workers; ++i) {
            int threads = odroid_threads[i] > 0 ? odroid_threads[i] : 1;
            worker_configs[i].thread_count = threads;
            if (!strcmp(odroid_mode, "cpu_only")) { per_worker_kind[i] = PW_CPU; per_worker_size[i] = odroid_sizes[i]; }
            else if (!strcmp(odroid_mode, "memory_only")) { per_worker_kind[i] = PW_MEM; per_worker_size[i] = odroid_sizes[i]; }
            else if (!strcmp(odroid_mode, "cpu_and_memory")) { per_worker_kind[i] = (i % 2 == 0 ? PW_CPU : PW_MEM); per_worker_size[i] = odroid_sizes[i]; }
            else if (!strcmp(odroid_mode, "memory_and_cpu")) { per_worker_kind[i] = (i % 2 == 0 ? PW_MEM : PW_CPU); per_worker_size[i] = odroid_sizes[i]; }
            else { per_worker_kind[i] = PW_CPU; per_worker_size[i] = odroid_sizes[i]; }
            worker_configs[i].workload = NULL;
        }
    } else {
        for (int i = 0; i < num_workers; ++i) {
            int threads = atoi(argv[3 + i]);
            if (threads <= 0) { fprintf(stderr, "Invalid thread count for worker %d. Using 1.\n", i); threads = 1; }
            worker_configs[i].thread_count = threads;
            per_worker_kind[i] = PW_UNSET;
            per_worker_size[i] = 0;
            worker_configs[i].workload = NULL;
        }
    }

    for (int i = 0; i < INPUT_BUFFER_SIZE; i++) input_buffer[i] = i + 1;

    pthread_t *workers = malloc(num_workers * sizeof(pthread_t));
    int *worker_ids = malloc(num_workers * sizeof(int));
    if (!workers || !worker_ids) { perror("malloc failed for worker threads"); return 1; }

    for (int i = 0; i < num_workers; i++) {
        worker_ids[i] = i;
        if (pthread_create(&workers[i], NULL, worker, &worker_ids[i]) != 0) { perror("pthread_create"); return 1; }
#ifdef USE_CUSTOM_AFFINITY
        int mapping_arr[num_workers];
        for (int j = 0; j < num_workers; j++) mapping_arr[j] = j;
        bind_thread_to_core_by_id(workers[i], i, mapping_arr, num_workers);
#else
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pid_t tid = (pid_t)syscall(SYS_gettid);
        int result = sched_setaffinity(tid, sizeof(cpu_set_t), &cpuset);
        if (result != 0) perror("sched_setaffinity");
        else printf("Thread %d bound to core %d\n", i, i);
        fflush(stdout);
#endif
    }

    for (int i = 0; i < num_workers; i++) pthread_join(workers[i], NULL);

    printf("All worker threads have completed. Updating final output.\n");
    fflush(stdout);

    printf("\nOutput buffer:\n");
    for (int i = 0; i < output_index; i++) printf("%d ", output_buffer[i]);
    printf("\n");
    fflush(stdout);

    int total_processing_threads = 0;
    for (int i = 0; i < num_workers; i++) total_processing_threads += worker_configs[i].thread_count;

    printf("\n--- Application Summary ---\n");
    if (parsed_odroid) {
        printf("Workload Type: %s\n", odroid_mode);
        printf("Chunk size: %d\n", chunk_size);
        printf("Number of workers: %d\n", num_workers);
        printf("Input buffer size: %d\n", INPUT_BUFFER_SIZE);
        printf("Output buffer size: %d\n", OUTPUT_BUFFER_SIZE);
        printf("Total processing threads: %d\n", total_processing_threads);
        printf("Workload Sizes: ");
        for (int i = 0; i < num_workers; ++i) printf("%zu ", per_worker_size[i]);
        printf("\n===========================\n");
    } else {
        printf("Chunk size: %d\n", chunk_size);
        printf("Number of workers: %d\n", num_workers);
        printf("Input buffer size: %d\n", INPUT_BUFFER_SIZE);
        printf("Output buffer size: %d\n", OUTPUT_BUFFER_SIZE);
        printf("Total processing threads: %d\n", total_processing_threads);
        fflush(stdout);
        printf("Exiting application.\n");
        fflush(stdout);
        fsync(1);
        free(workers); free(worker_ids); free(worker_configs);
        printf("==========================\n\n");
    }

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
