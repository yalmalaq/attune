#import time
#import sys
#import subprocess
#from threading import Thread
#
#values = []
#running = True
#
#def measure_thread():
#    tty = '/dev/ttyACM0'
#    try:
#        subprocess.run(f'stty -F {tty} 9600', shell=True, check=True)
#        with open(tty) as f:
#            for line in f:
#                if line.strip():  # Check if line is not empty
#                    values.append(float(line.split()[0]))
#                if not running:
#                    break
#    except Exception as e:
#        print(f"Error in measure_thread: {e}")
#
#def get_cpu_frequency(cpu):
#    try:
#        with open(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/cpuinfo_cur_freq", "r") as f:
#            return int(f.read().strip())
#    except FileNotFoundError:
#        return 0  # Return 0 if file is not found
#    except Exception as e:
#        print(f"Error in get_cpu_frequency: {e}")
#        return 0
#
#def measure_application(app):
#    global running
#    sum_freq = [0] * 8
#    counts = [0] * 8
#    all_freqs = [[] for _ in range(8)]
#
#    thread = Thread(target=measure_thread)
#    thread.start()
#
#    start_time = time.time()
#
#    # Run the application without cpu_list
#    app_cmd = app
#
#    # Updated perf command with valid event names
#    perf_ = (
#        f'perf stat -e cpu-clock '
#        f'-e task-clock '
#        f'-e cache-references '
#        f'-e cache-misses '
#        f'-e L1-dcache-load-misses '
#        f'-e L1-dcache-loads '
#        f'-e L1-dcache-store-misses '
#        f'-e L1-dcache-stores '
#        f'-e LLC-load-misses '
#        f'-e LLC-store-misses '
#        f'-e armv7_cortex_a15/l2d_cache/ '  # Correct event name
#        f'-e armv7_cortex_a15/l2d_cache_refill/ '  # Correct event name
#        f'-e armv7_cortex_a7/l2d_cache/ '  # Correct event name
#        f'-e armv7_cortex_a7/l2d_cache_refill/ '  # Correct event name
#        f'-e armv7_cortex_a15/bus_cycles/ '
#        f'-e armv7_cortex_a15/cpu_cycles/ '
#        f'{app_cmd}'
#    )
#
#    process = subprocess.Popen(perf_, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#
#    try:
#        while process.poll() is None:
#            for i in range(8):
#                freq = get_cpu_frequency(i)
#                sum_freq[i] += freq
#                counts[i] += 1
#                all_freqs[i].append(freq)
#            time.sleep(1)
#    except KeyboardInterrupt:
#        print("Application interrupted")
#        process.terminate()  # Terminate the application process
#    finally:
#        running = False  # Stop the measurement thread
#        thread.join()  # Wait for thread to complete
#        end_time = time.time()
#
#    total_time = end_time - start_time
#    avg = sum(values) / len(values)
#    energy = avg * total_time
#    energy_base = energy * 0.60
##power= avg * total_time
#
#    print('Power: ' + str(avg))
#    print('Energy: ' + str(energy))
#    #print('Energy_baseline: ' + str(energy_base))
#    print('Time: ' + str(total_time))
#    print('samples',len(values))
#
#    print('Power ' + 'Energy '  + 'Time' )
#    print(str(avg) + ' ' +str(energy)  + ' '  + str(total_time))
#    for i in range(8):
#        if counts[i] > 0:
#            avg_freq = sum_freq[i] / counts[i]
#            print(f"Average frequency of cpu{i}: {avg_freq:.2f} kHz")
#        else:
#            print(f"No measurements for cpu{i}")
#
#    for i in range(8):
#        print(f"All frequencies used by cpu{i}: {all_freqs[i]}")
#
#    stdout, stderr = process.communicate()
#    print("Perf output: ")
#    print(stdout.decode())
#    # print(stderr.decode())
#
#if __name__ == "__main__":
#    if len(sys.argv) != 2:
#        print(f"Usage: Python3 {sys.argv[0]} <./application>")
#        sys.exit(1)
#
#    app = sys.argv[1]
#    measure_application(app)


################################################ the belwo version is working fine, but the time measures only for the execution not measures the total runtime of the C application, including any overhead from initialisation, queuing, and processing. ################################################


#import time
#import sys
#import subprocess
#from threading import Thread
#
#values = []
#running = True
#
#def measure_thread():
#    tty = '/dev/ttyACM0'
#    try:
#        subprocess.run(f'stty -F {tty} 9600', shell=True, check=True)
#        with open(tty) as f:
#            for line in f:
#                if line.strip():  # Check if line is not empty
#                    values.append(float(line.split()[0]))
#                if not running:
#                    break
#    except Exception as e:
#        print(f"Error in measure_thread: {e}")
#
#def get_cpu_frequency(cpu):
#    try:
#        with open(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/cpuinfo_cur_freq", "r") as f:
#            return int(f.read().strip())
#    except FileNotFoundError:
#        return 0  # Return 0 if file is not found
#    except Exception as e:
#        print(f"Error in get_cpu_frequency: {e}")
#        return 0
#
#def measure_application(app_args):
#    global running
#    sum_freq = [0] * 8
#    counts = [0] * 8
#    all_freqs = [[] for _ in range(8)]
#
#    thread = Thread(target=measure_thread)
#    thread.start()
#
#    start_time = time.time()
#
#    # Prepare the perf command
#    app_cmd = " ".join(app_args)
#
#    # Updated perf command with valid event names
#    perf_ = (
#        f'perf stat -e cpu-clock '
#        f'-e task-clock '
#        f'-e cache-references '
#        f'-e cache-misses '
#        f'-e L1-dcache-load-misses '
#        f'-e L1-dcache-loads '
#        f'-e L1-dcache-store-misses '
#        f'-e L1-dcache-stores '
#        f'-e LLC-load-misses '
#        f'-e LLC-store-misses '
#        f'-e armv7_cortex_a15/l2d_cache/ '  # Correct event name
#        f'-e armv7_cortex_a15/l2d_cache_refill/ '  # Correct event name
#        f'-e armv7_cortex_a7/l2d_cache/ '  # Correct event name
#        f'-e armv7_cortex_a7/l2d_cache_refill/ '  # Correct event name
#        f'-e armv7_cortex_a15/bus_cycles/ '
#        f'-e armv7_cortex_a15/cpu_cycles/ '
#        f'{app_cmd}'
#    )
#
#    process = subprocess.Popen(perf_, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#
#    try:
#        while process.poll() is None:
#            for i in range(8):
#                freq = get_cpu_frequency(i)
#                sum_freq[i] += freq
#                counts[i] += 1
#                all_freqs[i].append(freq)
#            time.sleep(1)
#    except KeyboardInterrupt:
#        print("Application interrupted")
#        process.terminate()  # Terminate the application process
#    finally:
#        running = False  # Stop the measurement thread
#        thread.join()  # Wait for thread to complete
#        end_time = time.time()
#
#    total_time = end_time - start_time
#    avg = sum(values) / len(values)
#    energy = avg * total_time
#    energy_base = energy * 0.60
##power= avg * total_time
#
#    print('Power: ' + str(avg))
#    print('Energy: ' + str(energy))
#    #print('Energy_baseline: ' + str(energy_base))
#    print('Time: ' + str(total_time))
#    print('samples',len(values))
#
#    print('Power ' + 'Energy '  + 'Time' )
#    print(str(avg) + ' ' +str(energy)  + ' '  + str(total_time))
#    for i in range(8):
#        if counts[i] > 0:
#            avg_freq = sum_freq[i] / counts[i]
#            print(f"Average frequency of cpu{i}: {avg_freq:.2f} kHz")
#        else:
#            print(f"No measurements for cpu{i}")
#
#    for i in range(8):
#        print(f"All frequencies used by cpu{i}: {all_freqs[i]}")
#
#    stdout, stderr = process.communicate()
#    print("Perf output: ")
#    print(stdout.decode())
#    # print(stderr.decode())
#
#if __name__ == "__main__":
#    if len(sys.argv) < 2:
#        print(f"Usage: Python3 {sys.argv[0]} <./application> [args]")
#        sys.exit(1)
#
#    # Collect all arguments after the script name
#    app_args = sys.argv[1:]
#    measure_application(app_args)


################################################ the belwo version measures the total runtime of the C application, including any overhead from initialisation, queuing, and processing. ################################################

import time
import sys
import subprocess
from threading import Thread

values = []
running = True

def measure_thread():
    tty = '/dev/ttyACM0'
    try:
        subprocess.run(f'stty -F {tty} 9600', shell=True, check=True)
        with open(tty) as f:
            for line in f:
                if line.strip():  # Check if line is not empty
                    values.append(float(line.split()[0]))
                if not running:
                    break
    except Exception as e:
        print(f"Error in measure_thread: {e}")

def get_cpu_frequency(cpu):
    try:
        with open(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/cpuinfo_cur_freq", "r") as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return 0  # Return 0 if file is not found
    except Exception as e:
        print(f"Error in get_cpu_frequency: {e}")
        return 0

def measure_application(app_args):
    global running
    sum_freq = [0] * 8
    counts = [0] * 8
    all_freqs = [[] for _ in range(8)]

    thread = Thread(target=measure_thread)
    thread.start()

    # Prepare the perf command
    app_cmd = " ".join(app_args)

    # Updated perf command with valid event names
    perf_ = (
        f'perf stat -e cpu-clock '
        f'-e task-clock '
        f'-e cache-references '
        f'-e cache-misses '
        f'-e L1-dcache-load-misses '
        f'-e L1-dcache-loads '
        f'-e L1-dcache-store-misses '
        f'-e L1-dcache-stores '
        f'-e LLC-load-misses '
        f'-e LLC-store-misses '
        f'-e armv7_cortex_a15/l2d_cache/ '
        f'-e armv7_cortex_a15/l2d_cache_refill/ '
        f'-e armv7_cortex_a7/l2d_cache/ '
        f'-e armv7_cortex_a7/l2d_cache_refill/ '
        f'-e armv7_cortex_a15/bus_cycles/ '
        f'-e armv7_cortex_a15/cpu_cycles/ '
        f'{app_cmd}'
    )

    start_time = time.time()  # Start timing

    # Run the application and measure CPU frequency in parallel
    process = subprocess.Popen(perf_, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    try:
        while process.poll() is None:
            for i in range(8):
                freq = get_cpu_frequency(i)
                sum_freq[i] += freq
                counts[i] += 1
                all_freqs[i].append(freq)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Application interrupted")
        process.terminate()  # Terminate the application process
    finally:
        running = False  # Stop the measurement thread
        thread.join()  # Wait for the measurement thread to complete

    stdout, stderr = process.communicate()  # Wait for the process to finish

    end_time = time.time()  # End timing
    total_time = end_time - start_time

    avg = sum(values) / len(values) if values else 0  # Avoid division by zero
    energy = avg * total_time
    energy_base = energy * 0.60

    print('Power: ' + str(avg))
    print('Energy: ' + str(energy))
    print('Time: ' + str(total_time))
    print('samples', len(values))

    print('Power ' + 'Energy ' + 'Time')
    print(str(avg) + ' ' + str(energy) + ' ' + str(total_time))

    for i in range(8):
        if counts[i] > 0:
            avg_freq = sum_freq[i] / counts[i]
            print(f"Average frequency of cpu{i}: {avg_freq:.2f} kHz")
        else:
            print(f"No measurements for cpu{i}")

    for i in range(8):
        print(f"All frequencies used by cpu{i}: {all_freqs[i]}")

    # Print perf output after the process is done
    print("Perf output: ")
    print(stdout.decode())
    # print(stderr.decode())

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: Python3 {sys.argv[0]} <./application> [args]")
        sys.exit(1)

    # Collect all arguments after the script name
    app_args = sys.argv[1:]
    measure_application(app_args)
