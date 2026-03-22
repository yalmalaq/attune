#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sched.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdatomic.h>

#define FORCE_CPU_BINDING
#define MAX_WORKERS 32
#define INPUT_BUFFER_SIZE 100
#define OUTPUT_BUFFER_SIZE 100
#define DEBUG_INTERVAL 10

#ifdef FORCE_CPU_BINDING
static int *available_cores = NULL;
static int num_available_cores = 0;

static int parse_core_list(const char *buf, int **cores) {
    int count = 0;
    char *dup = strdup(buf), *tok = strtok(dup, ",\n");
    while (tok) {
        int s, e;
        if (sscanf(tok, "%d-%d", &s, &e) == 2) count += (e - s + 1);
        else if (sscanf(tok, "%d", &s) == 1) count++;
        tok = strtok(NULL, ",\n");
    }
    free(dup);

    *cores = malloc(count * sizeof(int));
    dup = strdup(buf); tok = strtok(dup, ",\n");
    int idx = 0;
    while (tok) {
        int s, e;
        if (sscanf(tok, "%d-%d", &s, &e) == 2) for (int i = s; i <= e; i++) (*cores)[idx++] = i;
        else if (sscanf(tok, "%d", &s) == 1) (*cores)[idx++] = s;
        tok = strtok(NULL, ",\n");
    }
    free(dup);
    return count;
}

static int get_online_cores(int **cores) {
    FILE *f = fopen("/sys/devices/system/cpu/online", "r");
    if (!f) { perror("open cpu online"); exit(EXIT_FAILURE); }
    char buf[256];
    if (!fgets(buf, sizeof(buf), f)) { fclose(f); fprintf(stderr,"Failed reading cpu online\n"); exit(EXIT_FAILURE); }
    fclose(f);
    return parse_core_list(buf, cores);
}

static void bind_thread_to_core(int idx) {
    if (!available_cores || num_available_cores <= 0) return;
    int core = available_cores[idx % num_available_cores];
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) == 0)
        printf("Thread %lu -> Core %d\n", (unsigned long)pthread_self(), core);
}
#define MAYBE_BIND(i) bind_thread_to_core(i)
#else
#define MAYBE_BIND(i) /* no-op */
#endif

// ------------------------------------------------------------------------
// GLOBAL VARIABLES
typedef enum { MODE_CPU_ONLY, MODE_MEM_ONLY, MODE_CPU_AND_MEM, MODE_MEM_AND_CPU } farm_mode_t;
static farm_mode_t mode;
static int num_workers, chunk_size;
static size_t cpu_loops[MAX_WORKERS], mem_sizes[MAX_WORKERS];
static int threads_per_worker[MAX_WORKERS];
static int input_buffer[INPUT_BUFFER_SIZE], output_buffer[OUTPUT_BUFFER_SIZE];
static int input_index = 0, output_index = 0;
pthread_mutex_t input_mutex = PTHREAD_MUTEX_INITIALIZER, output_mutex = PTHREAD_MUTEX_INITIALIZER;

// ------------------------------------------------------------------------
// DYNAMIC WORKLOADS
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
    free(arr);
    (void)s;
}

// ------------------------------------------------------------------------
// BACKWARD COMPATIBLE WRAPPERS (cpu1..cpu8, memory1..memory8)
#define DEFINE_CPU(N) void cpu##N(void){ cpu_workload(cpu_loops[(N)-1]); }
#define DEFINE_MEM(N) void memory##N(void){ memory_workload(mem_sizes[(N)-1]); }
DEFINE_CPU(1) DEFINE_CPU(2) DEFINE_CPU(3) DEFINE_CPU(4)
DEFINE_CPU(5) DEFINE_CPU(6) DEFINE_CPU(7) DEFINE_CPU(8)
DEFINE_MEM(1) DEFINE_MEM(2) DEFINE_MEM(3) DEFINE_MEM(4)
DEFINE_MEM(5) DEFINE_MEM(6) DEFINE_MEM(7) DEFINE_MEM(8)

// Arrays for compatibility
typedef void (*workload_func_t)(void);
workload_func_t cpu_funcs[8] = {cpu1,cpu2,cpu3,cpu4,cpu5,cpu6,cpu7,cpu8};
workload_func_t mem_funcs[8] = {memory1,memory2,memory3,memory4,memory5,memory6,memory7,memory8};

// ------------------------------------------------------------------------
// UNIFIED WORKER TASK (LIKE PIPELINE)
void run_worker_task(int id) {
    if (mode == MODE_CPU_ONLY) cpu_workload(cpu_loops[id]);
    else if (mode == MODE_MEM_ONLY) memory_workload(mem_sizes[id]);
    else if (mode == MODE_CPU_AND_MEM) (id % 2 == 0) ? cpu_workload(cpu_loops[id]) : memory_workload(mem_sizes[id]);
    else if (mode == MODE_MEM_AND_CPU) (id % 2 == 0) ? memory_workload(mem_sizes[id]) : cpu_workload(cpu_loops[id]);
}

// ------------------------------------------------------------------------
// WORKER CONFIG
typedef struct { int threads; workload_func_t legacy_workload; } worker_cfg;
worker_cfg workers_cfg[MAX_WORKERS];

// ------------------------------------------------------------------------
// THREAD FUNCTIONS
typedef struct { int start, end, worker_id, sub_idx; } sub_task;

void *sub_worker(void *arg) {
    sub_task *st = arg;
    MAYBE_BIND(st->worker_id + num_workers + st->sub_idx);
    for (int i = st->start; i < st->end; i++) run_worker_task(st->worker_id);
    free(st);
    return NULL;
}

void *worker_fn(void *arg) {
    int id = *(int*)arg;
    int tc = workers_cfg[id].threads;
    MAYBE_BIND(id);
    int chunks = 0;
    while (1) {
        int cnt = 0;
        int local_buf[INPUT_BUFFER_SIZE];
        // If chunk_size == 0 treat as unbounded: read as many inputs as available in one go
        if (chunk_size == 0) {
            pthread_mutex_lock(&input_mutex);
            while (input_index < INPUT_BUFFER_SIZE && cnt < INPUT_BUFFER_SIZE) local_buf[cnt++] = input_buffer[input_index++];
            pthread_mutex_unlock(&input_mutex);
        } else {
            pthread_mutex_lock(&input_mutex);
            while (cnt < chunk_size && input_index < INPUT_BUFFER_SIZE) local_buf[cnt++] = input_buffer[input_index++];
            pthread_mutex_unlock(&input_mutex);
        }
        if (!cnt) break;
        chunks++;

        // single-threaded worker behaviour if tc == 1
        if (tc <= 1) {
            for (int i = 0; i < cnt; ++i) run_worker_task(id);
        } else {
            // split work among tc threads: current thread does first slice, spawn tc-1 subworkers
            int base = cnt / tc;
            int rem = cnt % tc;
            int offset = 0;
            int first_end = base + (rem > 0 ? 1 : 0);
            for (int i = 0; i < first_end; ++i) run_worker_task(id);
            offset = first_end;
            pthread_t *subs = calloc(tc - 1, sizeof(pthread_t));
            for (int t = 1; t < tc; ++t) {
                int this_count = base + (t < rem ? 1 : 0);
                sub_task *st = malloc(sizeof(*st));
                st->start = offset;
                st->end = offset + this_count;
                st->worker_id = id;
                st->sub_idx = t;
                pthread_create(&subs[t-1], NULL, sub_worker, st);
                offset += this_count;
            }
            for (int t = 1; t < tc; ++t) pthread_join(subs[t-1], NULL);
            free(subs);
        }

        // write outputs (preserve original semantics)
        pthread_mutex_lock(&output_mutex);
        for (int i = 0; i < cnt && output_index < OUTPUT_BUFFER_SIZE; ++i) output_buffer[output_index++] = local_buf[i];
        pthread_mutex_unlock(&output_mutex);

        if (chunks % DEBUG_INTERVAL == 0) printf("Worker %d processed %d chunks\n", id, chunks);
    }
    printf("Worker %d exiting\n", id);
    return NULL;
}

// ------------------------------------------------------------------------
// MAIN
int main(int argc, char **argv) {
#ifdef FORCE_CPU_BINDING
    num_available_cores = get_online_cores(&available_cores);
#endif

    // Minimal CLI: ./farm <num_workers> <mode> <size1>...<sizeN>
    // Full CLI (original style): ./farm <num_workers> <mode> <size1>...<sizeN> <chunk_size> <threads1>...<threadsN>
    if (argc < 4) {
        fprintf(stderr,"Usage short: %s <num_workers> <mode> <size1>...<sizeN>\n",argv[0]);
        fprintf(stderr,"Usage full:  %s <num_workers> <mode> <size1>...<sizeN> <chunk_size> <threads1>...<threadsN>\n",argv[0]);
        exit(EXIT_FAILURE);
    }

    num_workers = atoi(argv[1]);
    if (num_workers <= 0 || num_workers > MAX_WORKERS) { fprintf(stderr,"num_workers out of range\n"); exit(EXIT_FAILURE); }

    char *m = argv[2];
    if      (!strcmp(m,"cpu_only")) mode = MODE_CPU_ONLY;
    else if (!strcmp(m,"memory_only")) mode = MODE_MEM_ONLY;
    else if (!strcmp(m,"cpu_and_memory")) mode = MODE_CPU_AND_MEM;
    else if (!strcmp(m,"memory_and_cpu")) mode = MODE_MEM_AND_CPU;
    else { fprintf(stderr,"Unknown mode %s\n",m); exit(EXIT_FAILURE); }

    // Determine if short form: argc == 3 + num_workers
    // Full form requires at least: 3 + num_workers + 1(chunk) + num_workers (threads) => 4 + 2*num_workers
    int expected_short = 3 + num_workers;
    int expected_full_min = 4 + 2 * num_workers;
    if (argc == expected_short) {
        // short form: set sizes from argv[3..], chunk_size=0 (unbounded), threads=1 each
        for (int i = 0; i < num_workers; ++i) {
            cpu_loops[i] = mem_sizes[i] = strtoull(argv[3 + i], NULL, 0);
            threads_per_worker[i] = 1;
        }
        chunk_size = 0;
        printf("Short form detected: treating as chunk_size=0 (unbounded) and threads=1 for each worker\n");
    } else if (argc >= expected_full_min) {
        // full form parsing
        for (int i = 0; i < num_workers; ++i) {
            cpu_loops[i] = mem_sizes[i] = strtoull(argv[3 + i], NULL, 0);
        }
        chunk_size = atoi(argv[3 + num_workers]);
        // threads follow
        for (int i = 0; i < num_workers; ++i) {
            threads_per_worker[i] = atoi(argv[4 + num_workers + i]);
            if (threads_per_worker[i] <= 0) threads_per_worker[i] = 1;
        }
    } else {
        fprintf(stderr, "Invalid argument count for %d workers\n", num_workers);
        exit(EXIT_FAILURE);
    }

    // Configure workers
    for (int i = 0; i < num_workers; ++i) {
        workers_cfg[i].threads = threads_per_worker[i];
        workers_cfg[i].legacy_workload = (i < 8) ? ((mode == MODE_MEM_ONLY) ? mem_funcs[i] : cpu_funcs[i]) : NULL;
    }

    // populate input buffer
    for (int i = 0; i < INPUT_BUFFER_SIZE; ++i) input_buffer[i] = i + 1;

    // Launch workers
    pthread_t th[MAX_WORKERS];
    int ids[MAX_WORKERS];
    for (int i = 0; i < num_workers; ++i) { ids[i] = i; pthread_create(&th[i], NULL, worker_fn, &ids[i]); }
    for (int i = 0; i < num_workers; ++i) pthread_join(th[i], NULL);

    int total_processing_threads = 0;
    for (int i = 0; i < num_workers; ++i) total_processing_threads += workers_cfg[i].threads;

    // Summary output
    printf("\n--- Application Summary ---\n");
    printf("Workload Type: %s\n", m);
    printf("Chunk size: %d\n", chunk_size);
    printf("Number of workers: %d\n", num_workers);
    printf("Input buffer size: %d\n", INPUT_BUFFER_SIZE);
    printf("Output buffer size: %d\n", OUTPUT_BUFFER_SIZE);
    printf("Total processing threads: %d\n", total_processing_threads);

    printf("Workload Sizes: ");
    for (int i = 0; i < num_workers; ++i) {
        printf("%zu ", (mode == MODE_MEM_ONLY) ? mem_sizes[i] : cpu_loops[i]);
    }
    printf("\n===========================\n");

#ifdef FORCE_CPU_BINDING
    if (available_cores) free(available_cores);
#endif
    return 0;
}
