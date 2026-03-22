#!/usr/bin/env python3
import sys
import os
import csv
import subprocess
import time
import re
import random
import gc
import traceback
from datetime import datetime
from pathlib import Path
import numpy as np
import pickle

random.seed(12345)
np.random.seed(12345)

RUNS = [
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000]*3, "obj": "energy"},
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000]*3, "obj": "time"},
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000]*3, "obj": "balanced"},
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [5_000_000]*3, "obj": "energy"},
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [5_000_000]*3, "obj": "time"},
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [5_000_000]*3, "obj": "balanced"},
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [10_000_000]*3, "obj": "energy"},
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [10_000_000]*3, "obj": "time"},
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [10_000_000]*3, "obj": "balanced"},
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000,2_000_000,3_000_000], "obj": "energy"},
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000,2_000_000,3_000_000], "obj": "time"},
    {"app": "pipeline", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000,2_000_000,3_000_000], "obj": "balanced"},
    {"app": "pipeline", "wl": "memory_only", "cnt": 4, "sizes": [2_000_000]*4, "obj": "energy"},
    {"app": "pipeline", "wl": "memory_only", "cnt": 4, "sizes": [2_000_000]*4, "obj": "time"},
    {"app": "pipeline", "wl": "memory_only", "cnt": 4, "sizes": [2_000_000]*4, "obj": "balanced"},
    {"app": "pipeline", "wl": "memory_only", "cnt": 4, "sizes": [4_000_000]*4, "obj": "energy"},
    {"app": "pipeline", "wl": "memory_only", "cnt": 4, "sizes": [4_000_000]*4, "obj": "time"},
    {"app": "pipeline", "wl": "memory_only", "cnt": 4, "sizes": [4_000_000]*4, "obj": "balanced"},
    {"app": "pipeline", "wl": "cpu_and_memory", "cnt": 2, "sizes": [8_000_000]*2, "obj": "energy"},
    {"app": "pipeline", "wl": "cpu_and_memory", "cnt": 2, "sizes": [8_000_000]*2, "obj": "time"},
    {"app": "pipeline", "wl": "cpu_and_memory", "cnt": 2, "sizes": [8_000_000]*2, "obj": "balanced"},
    {"app": "pipeline", "wl": "cpu_and_memory", "cnt": 2, "sizes": [16_000_000]*2, "obj": "energy"},
    {"app": "pipeline", "wl": "cpu_and_memory", "cnt": 2, "sizes": [16_000_000]*2, "obj": "time"},
    {"app": "pipeline", "wl": "cpu_and_memory", "cnt": 2, "sizes": [16_000_000]*2, "obj": "balanced"},
    {"app": "pipeline", "wl": "memory_and_cpu", "cnt": 2, "sizes": [8_000_000]*2, "obj": "energy"},
    {"app": "pipeline", "wl": "memory_and_cpu", "cnt": 2, "sizes": [8_000_000]*2, "obj": "time"},
    {"app": "pipeline", "wl": "memory_and_cpu", "cnt": 2, "sizes": [8_000_000]*2, "obj": "balanced"},
    {"app": "pipeline", "wl": "memory_and_cpu", "cnt": 2, "sizes": [16_000_000]*2, "obj": "energy"},
    {"app": "pipeline", "wl": "memory_and_cpu", "cnt": 2, "sizes": [16_000_000]*2, "obj": "time"},
    {"app": "pipeline", "wl": "memory_and_cpu", "cnt": 2, "sizes": [16_000_000]*2, "obj": "balanced"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000]*3, "obj": "energy"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000]*3, "obj": "time"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000]*3, "obj": "balanced"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [5_000_000]*3, "obj": "energy"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [5_000_000]*3, "obj": "time"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [5_000_000]*3, "obj": "balanced"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [10_000_000]*3, "obj": "energy"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [10_000_000]*3, "obj": "time"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [10_000_000]*3, "obj": "balanced"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000,2_000_000,3_000_000], "obj": "energy"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000,2_000_000,3_000_000], "obj": "time"},
    {"app": "farm", "wl": "cpu_only", "cnt": 3, "sizes": [1_000_000,2_000_000,3_000_000], "obj": "balanced"},
    {"app": "farm", "wl": "memory_only", "cnt": 4, "sizes": [2_000_000]*4, "obj": "energy"},
    {"app": "farm", "wl": "memory_only", "cnt": 4, "sizes": [2_000_000]*4, "obj": "time"},
    {"app": "farm", "wl": "memory_only", "cnt": 4, "sizes": [2_000_000]*4, "obj": "balanced"},
    {"app": "farm", "wl": "memory_only", "cnt": 4, "sizes": [4_000_000]*4, "obj": "energy"},
    {"app": "farm", "wl": "memory_only", "cnt": 4, "sizes": [4_000_000]*4, "obj": "time"},
    {"app": "farm", "wl": "memory_only", "cnt": 4, "sizes": [4_000_000]*4, "obj": "balanced"},
    {"app": "farm", "wl": "cpu_and_memory", "cnt": 2, "sizes": [8_000_000]*2, "obj": "energy"},
    {"app": "farm", "wl": "cpu_and_memory", "cnt": 2, "sizes": [8_000_000]*2, "obj": "time"},
    {"app": "farm", "wl": "cpu_and_memory", "cnt": 2, "sizes": [8_000_000]*2, "obj": "balanced"},
    {"app": "farm", "wl": "cpu_and_memory", "cnt": 2, "sizes": [16_000_000]*2, "obj": "energy"},
    {"app": "farm", "wl": "cpu_and_memory", "cnt": 2, "sizes": [16_000_000]*2, "obj": "time"},
    {"app": "farm", "wl": "cpu_and_memory", "cnt": 2, "sizes": [16_000_000]*2, "obj": "balanced"},
    {"app": "farm", "wl": "memory_and_cpu", "cnt": 2, "sizes": [8_000_000]*2, "obj": "energy"},
    {"app": "farm", "wl": "memory_and_cpu", "cnt": 2, "sizes": [8_000_000]*2, "obj": "time"},
    {"app": "farm", "wl": "memory_and_cpu", "cnt": 2, "sizes": [8_000_000]*2, "obj": "balanced"},
    {"app": "farm", "wl": "memory_and_cpu", "cnt": 2, "sizes": [16_000_000]*2, "obj": "energy"},
    {"app": "farm", "wl": "memory_and_cpu", "cnt": 2, "sizes": [16_000_000]*2, "obj": "time"},
    {"app": "farm", "wl": "memory_and_cpu", "cnt": 2, "sizes": [16_000_000]*2, "obj": "balanced"},
]

DEFAULT_RUN_CHOSEN = True
DEFAULT_RUN_ALTERNATIVES = True
DEFAULT_DO_COMPARE = True
COOL_DOWN_SEC = 5

DEFAULT_APP = "pipeline"
DEFAULT_WL = "cpu_only"
DEFAULT_CNT = 3
DEFAULT_SIZES = [5_000_000] * DEFAULT_CNT
DEFAULT_OBJ = "energy"

K = 5
n_candidates = 3000
repeat_count = 5
min_mem_kb = 180000
COOL_DOWN_SEC = 5

print("\nATTUNE Odroid — CLI Generator (batch RUNS)\n")
print(f"Runs configured: {len(RUNS)}\n")

def cores_to_mask(big_cores_on, little_cores_on):
    try:
        big = int(big_cores_on) if big_cores_on is not None else 0
    except Exception:
        big = 0
    try:
        little = int(little_cores_on) if little_cores_on is not None else 0
    except Exception:
        little = 0
    little = max(0, min(4, little))
    big = max(0, min(4, big))
    little_indices = list(range(0, 4))[:little]
    big_indices = list(range(4, 8))[:big]
    indices = little_indices + big_indices
    if not indices:
        return None
    if len(indices) == 1:
        return str(indices[0])
    runs = []
    start = prev = indices[0]
    for x in indices[1:]:
        if x == prev + 1:
            prev = x
            continue
        runs.append((start, prev))
        start = prev = x
    runs.append((start, prev))
    parts = []
    for a, b in runs:
        parts.append(f"{a}" if a == b else f"{a}-{b}")
    return ",".join(parts)

def set_cluster_freq(cluster, freq):
    if freq is None:
        return False
    try:
        freq_int = int(freq)
    except Exception:
        return False
    cids = list(range(0, 4)) if cluster == "little" else list(range(4, 8))
    success = False
    for cid in cids:
        base = f"/sys/devices/system/cpu/cpu{cid}/cpufreq"
        gov_path = os.path.join(base, "scaling_governor")
        set_path = os.path.join(base, "scaling_setspeed")
        try:
            if os.geteuid() == 0:
                with open(gov_path, "w") as f:
                    f.write("userspace\n")
                with open(set_path, "w") as f:
                    f.write(str(freq_int) + "\n")
                success = True
                continue
        except Exception:
            pass
        try:
            cmd_gov = f'echo "userspace" | sudo tee {gov_path} >/dev/null'
            subprocess.run(cmd_gov, shell=True, check=True)
            cmd_set = f'echo "{freq_int}" | sudo tee {set_path} >/dev/null'
            subprocess.run(cmd_set, shell=True, check=True)
            success = True
        except Exception:
            print(f"Failed to set cpu{cid} for cluster {cluster} to {freq_int} (best-effort)", file=sys.stderr)
    if success:
        print(f"set_cluster_freq({cluster}) attempted {freq_int} on cpus {cids}", file=sys.stderr)
    return success

def restore_default_governor():
    for cid in range(8):
        base = f"/sys/devices/system/cpu/cpu{cid}/cpufreq"
        if os.geteuid() == 0:
            try:
                with open(os.path.join(base, "scaling_governor"), "w") as f:
                    f.write("ondemand\n")
                continue
            except Exception:
                pass
        cmd = f'echo "ondemand" | sudo tee {base}/scaling_governor >/dev/null'
        try:
            subprocess.run(cmd, shell=True, check=False)
        except OSError:
            print(f"Failed to restore governor for cpu{cid}; continuing.")

def run_real_measurement_program_first(cli_prog_and_args, core_mask=None, little_freq=None, big_freq=None):
    print(f"\nRunning real measurement (program-first): {cli_prog_and_args}")
    time.sleep(0.25)
    if not cli_prog_and_args or not cli_prog_and_args.strip():
        print("Empty CLI string")
        return None, None, "", ""
    if little_freq is not None:
        set_cluster_freq("little", int(little_freq))
    if big_freq is not None:
        set_cluster_freq("big", int(big_freq))
    time.sleep(0.12 + COOL_DOWN_SEC)
    parts = cli_prog_and_args.strip().split()
    prog = parts[0]
    prog_args = parts[1:]
    base_cmd = ["sudo", "python3", "INA219_energy_freq.py", prog] + prog_args
    if core_mask:
        cmd = ["taskset", "-c", core_mask] + base_cmd
    else:
        cmd = base_cmd
    raw_cmd = " ".join(cmd)
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
        raw_stdout = out
        m_e = re.search(r"Energy:\s*([\d.]+)", out)
        m_t = re.search(r"Time:\s*([\d.]+)", out)
        if not m_e or not m_t:
            print("INA219 wrapper did not produce Energy/Time; capturing raw stdout for debugging")
            return None, None, raw_cmd, raw_stdout
        e = float(m_e.group(1))
        t = float(m_t.group(1))
        return e, t, raw_cmd, raw_stdout
    except subprocess.CalledProcessError as cpe:
        raw_stdout = cpe.output if hasattr(cpe, "output") else ""
        print(f"measurement failed (CalledProcessError): {cpe}; cmd: {raw_cmd}", file=sys.stderr)
        return None, None, raw_cmd, raw_stdout
    except Exception as ex:
        raw_stdout = traceback.format_exc()
        print(f"measurement failed: {ex}; cmd: {raw_cmd}", file=sys.stderr)
        return None, None, raw_cmd, raw_stdout

def run_real_measurement(cli_str, use_taskset=True):
    s = cli_str.strip()
    if not s:
        return None, None, "", ""
    parts = s.split()
    if parts[0].startswith("--"):
        prog_idx = None
        for i, tok in enumerate(parts):
            if tok.startswith("./") or tok.startswith("pipe_") or tok.startswith("farm") or tok.startswith("default_"):
                prog_idx = i
                break
        prog_cli = " ".join(parts[prog_idx:]) if prog_idx is not None else s
    else:
        prog_cli = s
    core_mask = "0-7" if use_taskset else None
    return run_real_measurement_program_first(prog_cli, core_mask=core_mask, little_freq=None, big_freq=None)

def log_calibration(raw_cat, raw_num, e_real, t_real):
    file  = "calib.csv"
    entry = {**raw_cat, **raw_num, "energy_real": e_real, "time_real": t_real}
    new   = not os.path.exists(file)
    with open(file, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(entry.keys()))
        if new:
            w.writeheader()
        w.writerow(entry)

def has_enough_memory(min_kb=min_mem_kb):
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    kb = int(line.split()[1])
                    return kb > min_kb
    except Exception:
        return False

def safe_set_cluster_freq(cluster, freq, min_kb=min_mem_kb):
    if not has_enough_memory(min_kb=min_mem_kb):
        print(f"Low memory: skipping set_cluster_freq({cluster},{freq}) to avoid fork error.")
        return False
    try:
        ok = set_cluster_freq(cluster, freq)
        if not ok:
            print(f"set_cluster_freq reported failure for {cluster}={freq}")
        time.sleep(0.05)
        return ok
    except OSError:
        print(f"OS error setting {cluster} freq; skipping.")
        return False

def load_model(app):
    with open(f"{app}_model_numpy.pkl","rb") as f:
        spec = pickle.load(f)
    return (
        spec["num_cols"], spec["num_means"],
        spec["cat_cols"], spec["categories"],
        spec["forests"],
        spec.get("alpha",[1.0,1.0]),
        spec.get("beta",[0.0,0.0])
    )

def load_configs(app):
    cols = Path(f"{app}_columns.txt").read_text().splitlines()
    cidx = {c:i for i,c in enumerate(cols)}
    npz  = Path(f"{app}_configs.npz")
    if npz.exists():
        data = np.load(str(npz), mmap_mode="r")
        pool = data["X"]
        print(f"Loaded {app}_configs.npz (shape={pool.shape})")
    else:
        pool = np.load(f"{app}_configs.npy", allow_pickle=True)
        print(f"Loaded {app}_configs.npy (shape={pool.shape})")
    return pool, cols, cidx

def predict_tree(node, x):
    while not node["leaf"]:
        node = node["left"] if x[node["feature"]] <= node["threshold"] else node["right"]
    return node["value"]

def predict_forest(trees, x):
    return sum(predict_tree(t, x) for t in trees) / len(trees)

def score_factory(num_cols, num_means, cat_cols, categories, forests, alpha, beta):
    def score(raw_cat, raw_num):
        X = []
        for col, mean in zip(num_cols, num_means):
            v = raw_num.get(col)
            X.append(v if isinstance(v, (int,float)) else mean)
        for col, cats in zip(cat_cols, categories):
            v = raw_cat.get(col,"")
            for cat in cats:
                X.append(1.0 if v==cat else 0.0)
        e_raw = predict_forest(forests[0], X)
        t_raw = predict_forest(forests[1], X)
        return alpha[0]*e_raw + beta[0], alpha[1]*t_raw + beta[1]
    return score

def safe_int(x, default=0):
    try:
        if x is None or x == "":
            return default
        return int(x)
    except Exception:
        return default

def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def format_cli(cfg, cidx, cnt, wl, app):
    big = safe_int(cfg[cidx.get("big_cores_on")], 0)
    little = safe_int(cfg[cidx.get("little_cores_on")], 0)
    lf_hz = safe_int(cfg[cidx.get("avg_little_freq")], 800000)
    bf_hz = safe_int(cfg[cidx.get("avg_big_freq")], 900000)
    LF_MIN, LF_MAX = 800_000, 1_500_000
    BF_MIN, BF_MAX = 900_000, 2_000_000
    lf_hz = max(LF_MIN, min(LF_MAX, lf_hz))
    bf_hz = max(BF_MIN, min(BF_MAX, bf_hz))
    lf_ghz = lf_hz / 1e6
    bf_ghz = bf_hz / 1e6

    if app == "pipeline":
        grouping = safe_int(cfg[cidx.get("grouping")], 0)
        grouping = max(1, grouping)
        stage_wls = [str(safe_int(cfg[cidx.get(f"workload_size_stage{i}")], 0)) for i in range(1, cnt + 1)]
        stage_qs  = [safe_int(cfg[cidx.get(f"queue{i}_size")], 0)      for i in range(1, cnt + 1)]
        stage_ts  = [safe_int(cfg[cidx.get(f"stage{i}_threads")], 0)  for i in range(1, cnt + 1)]
        if grouping == 1:
            first_group_size = 1
        else:
            first_group_size = 2
        groups = []
        if first_group_size > 0:
            groups.append(list(range(0, min(first_group_size, cnt))))
        idx = min(first_group_size, cnt)
        while idx < cnt:
            groups.append([idx]); idx += 1
        assert sum(len(g) for g in groups) == cnt, "group partitioning failed"
        q_tokens = []; t_tokens = []
        for grp in groups:
            first_idx = grp[0]
            q_val = max(1, safe_int(stage_qs[first_idx], 1))
            t_val = max(1, safe_int(stage_ts[first_idx], 1))
            q_tokens.append(str(q_val))
            t_tokens.append(str(t_val))
        tokens = []
        tokens += [f"./pipe_{cnt}stages", wl]
        tokens += stage_wls
        tokens += [str(grouping)]
        tokens += q_tokens
        tokens += t_tokens
        expected_qt_len = cnt if grouping == 1 else len(groups)
        assert len(q_tokens) == expected_qt_len
        assert len(t_tokens) == expected_qt_len
        return (
            f"--big-cores {big} --little-cores {little} "
            f"--little-freq {lf_ghz:.1f}GHz --big-freq {bf_ghz:.1f}GHz "
            + " ".join(tokens)
        )

    workers = cnt
    chunk = safe_int(cfg[cidx.get("chunk_size")], 1)
    wls = []; thrs = []
    for i in range(1, workers + 1):
        wl_ = safe_int(cfg[cidx.get(f"workload_size_w{i}")], 0)
        thr = safe_int(cfg[cidx.get(f"w{i}_threads")], 1)
        thr = max(1, thr)
        wls.append(str(wl_))
        thrs.append(str(thr))

    return (
        f"--big-cores {big} --little-cores {little} "
        f"--little-freq {lf_ghz:.1f}GHz --big-freq {bf_ghz:.1f}GHz "
        f"./farm {workers} {wl} " +
        " ".join(wls) + f" {chunk} " +
        " ".join(thrs)
    )

def parse_cli_for_mask_and_freq(cli):
    toks = cli.strip().split()
    big_val = None; little_val = None
    lf_hz = None; bf_hz = None
    for i, tok in enumerate(toks):
        if tok == "--big-cores" and i + 1 < len(toks):
            try: big_val = int(toks[i+1])
            except: pass
        if tok == "--little-cores" and i + 1 < len(toks):
            try: little_val = int(toks[i+1])
            except: pass
        if tok == "--little-freq" and i + 1 < len(toks):
            v = toks[i+1]
            if v.endswith("GHz"):
                try: lf_hz = int(float(v[:-3]) * 1e6)
                except: pass
            else:
                try: lf_hz = int(float(v))
                except: pass
        if tok == "--big-freq" and i + 1 < len(toks):
            v = toks[i+1]
            if v.endswith("GHz"):
                try: bf_hz = int(float(v[:-3]) * 1e6)
                except: pass
            else:
                try: bf_hz = int(float(v))
                except: pass
    mask = cores_to_mask(big_val, little_val)
    return mask, lf_hz, bf_hz, big_val, little_val

def random_candidates(pool, cidx, app, wl, cnt, sizes, n):
    candidates = []
    mask = (pool[:, cidx["workload_type"]] == wl)
    key = "num_stages" if app == "pipeline" else "number_of_workers"
    mask &= (pool[:, cidx[key]].astype(int) == cnt)
    for i, sz in enumerate(sizes, start=1):
        col = (f"workload_size_stage{i}" if app == "pipeline" else f"workload_size_w{i}")
        mask &= (pool[:, cidx[col]].astype(int) == sz)
    real_rows = pool[mask]
    if real_rows.shape[0] > 0:
        return [("real", row) for row in real_rows]
    if app == "pipeline":
        result = []
        for row in pool:
            cfg = np.array(row, dtype=object, copy=True)
            for i_sz, sz in enumerate(sizes, start=1):
                cfg[cidx[f"workload_size_stage{i_sz}"]] = sz
            result.append(("rand", cfg))
            if len(result) >= n:
                break
        return result
    combos = [(b, l) for b in range(5) for l in range(5) if b + l > 0]
    LITTLE_FREQS = [800_000 + 100_000 * i for i in range(8)]
    BIG_FREQS = list(range(900_000, 2_000_001, 100_000))
    PHYSICAL_CORES = 8
    while len(candidates) < n:
        b_on, l_on = random.choice(combos)
        lf = random.choice(LITTLE_FREQS)
        bf = random.choice(BIG_FREQS)
        active_cores = max(b_on + l_on, 1)
        max_total_threads = min(active_cores, PHYSICAL_CORES)
        if max_total_threads < cnt:
            total_thr = cnt
        else:
            total_thr = random.randint(cnt, max_total_threads)
        raw_alloc = list(np.random.multinomial(total_thr - cnt, [1/cnt] * cnt)) if total_thr > cnt else [0] * cnt
        alloc = [a + 1 for a in raw_alloc]
        cfg = np.empty(pool.shape[1], dtype=object)
        cfg[cidx["workload_type"]] = wl
        cfg[cidx[key]] = cnt
        cfg[cidx["core_binding_type"]] = "manual_roundrobin"
        cfg[cidx["workload_balance"]] = "unknown"
        cfg[cidx["big_cores_on"]] = b_on
        cfg[cidx["little_cores_on"]] = l_on
        cfg[cidx["avg_little_freq"]] = lf
        cfg[cidx["avg_big_freq"]] = bf
        cfg[cidx["chunk_size"]] = random.choice([1, 2, 4, 8, 16, 32, 64, 128])
        for i_sz, sz in enumerate(sizes, start=1):
            cfg[cidx[f"workload_size_w{i_sz}"]] = sz
        for i_thr, t in enumerate(alloc, start=1):
            cfg[cidx[f"w{i_thr}_threads"]] = max(0, int(t))
        candidates.append(("rand", cfg))
    return candidates

def recommend_all(pool, cols, cidx, app, wl, cnt, sizes, n_candidates, K,
                  num_cols, num_means, cat_cols, categories, forests, alpha, beta):
    score = score_factory(num_cols, num_means, cat_cols, categories, forests, alpha, beta)
    cand = random_candidates(pool, cidx, app, wl, cnt, sizes, n_candidates)
    if not cand:
        print("No candidates generated. Check pool, sizes and cnt; cannot recommend.")
        return {}, {"energy": [], "time": [], "balanced": []}
    scored = []
    for tag, cfg in cand:
        raw_num = {}
        raw_cat = {}
        for c in num_cols:
            try:
                raw_num[c] = cfg[cidx[c]]
            except Exception:
                raw_num[c] = None
        for c in cat_cols:
            try:
                raw_cat[c] = cfg[cidx[c]]
            except Exception:
                raw_cat[c] = ""
        e, t = score(raw_cat, raw_num)
        scored.append((tag, cfg, float(e), float(t)))
    E = np.array([s[2] for s in scored]) if scored else np.array([])
    T = np.array([s[3] for s in scored]) if scored else np.array([])
    if E.size == 0 or T.size == 0:
        print("Scoring produced no numeric results; aborting.")
        return {}, {"energy": [], "time": [], "balanced": []}
    Emax = E.max() if E.size else 1.0
    Tmax = T.max() if T.size else 1.0
    Enorm = E / Emax if Emax != 0 else E
    Tnorm = T / Tmax if Tmax != 0 else T
    bal = Enorm + Tnorm
    n = len(scored)
    front = []
    for i in range(n):
        e_i, t_i = E[i], T[i]
        dominated = False
        for j in range(n):
            if j == i:
                continue
            e_j, t_j = E[j], T[j]
            if (e_j <= e_i and t_j <= t_i) and (e_j < e_i or t_j < t_i):
                dominated = True
                break
        if not dominated:
            front.append(i)
    if not front:
        print("No Pareto-optimal candidates found; using full set for selection.")
        front = list(range(n))
    results = {}
    alts = {}
    def pick_best_index(metric):
        if metric == "energy":
            key = lambda i: E[i]
        elif metric == "time":
            key = lambda i: T[i]
        else:
            key = lambda i: bal[i]
        try:
            return min(front, key=key)
        except Exception:
            return front[0] if front else 0
    for metric in ("energy", "time", "balanced"):
        best_idx = pick_best_index(metric)
        best_cfg = scored[best_idx][1]
        try:
            results[metric] = format_cli(best_cfg, cidx, cnt, wl, app)
        except Exception:
            results[metric] = ""
        if metric == "energy":
            order = np.argsort(E)
        elif metric == "time":
            order = np.argsort(T)
        else:
            order = np.argsort(bal)
        seen, uniq = set(), []
        for i in order:
            if i == best_idx:
                continue
            try:
                cli = format_cli(scored[i][1], cidx, cnt, wl, app)
            except Exception:
                continue
            if not cli or cli in seen:
                continue
            seen.add(cli); uniq.append(cli)
            if len(uniq) >= K:
                break
        alts[metric] = uniq
    return results, alts

for run_idx, run in enumerate(RUNS, start=1):
    app = run.get("app", DEFAULT_APP)
    wl = run.get("wl", DEFAULT_WL)
    cnt = run.get("cnt", DEFAULT_CNT)
    sizes = run.get("sizes", DEFAULT_SIZES)
    obj = run.get("obj", DEFAULT_OBJ)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\n" + "="*70)
    print(f"Run {run_idx}/{len(RUNS)} — app={app} wl={wl} cnt={cnt} sizes={sizes} obj={obj}")
    print("="*70 + "\n")

    try:
        num_cols, num_means, cat_cols, categories, forests, alpha, beta = load_model(app)
        pool, cols, cidx = load_configs(app)
    except Exception as ex:
        print(f"Failed to load model/config for app={app}: {ex}", file=sys.stderr)
        traceback.print_exc()
        continue

    key = "num_stages" if app == "pipeline" else "number_of_workers"
    mask = (pool[:, cidx["workload_type"]] == wl) & (pool[:, cidx[key]].astype(int) == cnt)
    for i_sz, sz in enumerate(sizes, start=1):
        col = (f"workload_size_stage{i_sz}" if app=="pipeline" else f"workload_size_w{i_sz}")
        mask &= (pool[:, cidx[col]].astype(int) == sz)
    filtered = pool[mask]
    print(f"  {filtered.shape[0]} configs match app={app}, wl={wl}, cnt={cnt}, sizes={sizes}")

    results, alts = recommend_all(pool, cols, cidx, app, wl, cnt, sizes, n_candidates, K,
                                  num_cols, num_means, cat_cols, categories, forests, alpha, beta)
    if not results:
        print("No recommendation produced for this run; skipping.")
        continue
    if obj not in results:
        print(f"No recommendation for objective '{obj}' in this run; skipping.")
        continue
    best_cli = results[obj]
    alt_clis = alts.get(obj, [])
    if not best_cli:
        print("Recommendation produced an empty CLI; skipping.")
        continue
    print("\nRecommended Config CLI:\n", best_cli)

    try:
        del forests, pool, filtered, num_cols, num_means, cat_cols, categories, cols
    except Exception:
        pass
    gc.collect()
    time.sleep(0.25)

    run_best = DEFAULT_RUN_CHOSEN
    e_chosen = t_chosen = None
    raw_chosen_cmd = ""
    raw_chosen_out = ""

    if run_best:
        gc.collect()
        if not has_enough_memory(min_kb=min_mem_kb):
            print("Low memory detected. Skipping real measurement to avoid fork failure.")
        else:
            safe_set_cluster_freq("little", 0)
            safe_set_cluster_freq("big", 0)
            time.sleep(COOL_DOWN_SEC)

            parts = best_cli.strip().split()
            if parts and parts[0].startswith("--"):
                prog_idx = None
                for i, tok in enumerate(parts):
                    if tok.startswith("./") or tok.startswith("pipe_") or tok.startswith("farm") or tok.startswith("default_"):
                        prog_idx = i
                        break
                prog_cli = " ".join(parts[prog_idx:]) if prog_idx is not None else best_cli
            else:
                prog_cli = best_cli

            mask, lf_hz, bf_hz, _, _ = parse_cli_for_mask_and_freq(best_cli)
            e_chosen, t_chosen, raw_chosen_cmd, raw_chosen_out = run_real_measurement_program_first(
                prog_cli, core_mask=mask or "0-7", little_freq=lf_hz, big_freq=bf_hz)

    if run_best and DEFAULT_RUN_ALTERNATIVES and alt_clis:
        sizes_str = "_".join(str(s) for s in sizes)
        real_file = f"real_vs_real_{app}_{obj}_{wl}_{cnt}_{sizes_str}_{timestamp}.txt"
        with open(real_file, "w") as f:
            f.write(">>> Real measurements on alternatives\n\n")
            f.write(">>> Chosen config\n")
            f.write(f"{best_cli}\n")
            f.write(f"Raw chosen command: {raw_chosen_cmd}\n")
            if raw_chosen_out:
                f.write("\n>>> Raw INA219 stdout for chosen config <<<\n")
                f.write(raw_chosen_out + "\n\n")
            if e_chosen is not None:
                f.write(f"  Real Energy={e_chosen:.4f}, Time={t_chosen:.4f}\n\n")
            f.write(">>> Alternatives vs Chosen\n\n")
            for cli in alt_clis:
                f.write(f"{cli}\n")
                gc.collect()
                if not has_enough_memory(min_kb=min_mem_kb):
                    print("Low memory: skipping measurement for an alternative to avoid fork failure.")
                    f.write("  measurement skipped (low memory)\n\n")
                    continue
                parts = cli.strip().split()
                if parts and parts[0].startswith("--"):
                    prog_idx = None
                    for i, tok in enumerate(parts):
                        if tok.startswith("./") or tok.startswith("pipe_") or tok.startswith("farm") or tok.startswith("default_"):
                            prog_idx = i
                            break
                    prog_cli = " ".join(parts[prog_idx:]) if prog_idx is not None else cli
                else:
                    prog_cli = cli
                mask, lf_hz, bf_hz, big_cnt, little_cnt = parse_cli_for_mask_and_freq(cli)
                prefer_mask = mask or "0-7"
                e_i, t_i, raw_cmd_i, raw_out_i = run_real_measurement_program_first(
                    prog_cli, core_mask=prefer_mask, little_freq=lf_hz, big_freq=bf_hz)
                if raw_cmd_i:
                    f.write(f"  Raw cmd: {raw_cmd_i}\n")
                if raw_out_i:
                    f.write("\n  Raw INA219 stdout:\n")
                    f.write(raw_out_i + "\n")
                if e_i is None:
                    f.write("  measurement failed\n\n")
                    continue
                if e_chosen is None:
                    de = 0.0; dt = 0.0
                else:
                    de = 100 * (e_i - e_chosen) / e_chosen
                    dt = 100 * (t_i - t_chosen) / t_chosen
                f.write(
                    f"  Real Energy={e_i:.4f} ({'+' if de>=0 else ''}{de:.1f}%), "
                    f"Time={t_i:.4f} ({'+' if dt>=0 else ''}{dt:.1f}%)\n\n"
                )
                log_calibration({}, {}, e_i, t_i)
        print(f"Wrote real-vs-real report → {real_file}")

    if DEFAULT_DO_COMPARE:
        default_cli_base = (f"./default_farm {cnt} {wl} " + " ".join(str(s) for s in sizes)) if app == "farm" else (f"./default_pipe_{cnt}stages {wl} " + " ".join(str(s) for s in sizes))
        sizes_str = "_".join(str(s) for s in sizes)
        csv_name = f"compare_default_vs_cli_{app}_{obj}_{wl}_{cnt}_{sizes_str}_{timestamp}.csv"
        report_name = f"compare_report_{app}_{obj}_{wl}_{cnt}_{sizes_str}_{timestamp}.txt"
        with open(csv_name, "w", newline="") as csvf:
            fieldnames = ["variant","repeat","cli","energy_j","time_s","timestamp","notes","raw_cmd","raw_stdout"]
            writer = csv.DictWriter(csvf, fieldnames=fieldnames)
            writer.writeheader()
            rows = []
            print(f"\nRunning recommended CLI {repeat_count} times (B).")
            rec_mask, rec_lf_hz, rec_bf_hz, _, _ = parse_cli_for_mask_and_freq(best_cli)
            rec_mask = rec_mask or "0-7"
            for rep in range(1, repeat_count + 1):
                gc.collect()
                if not has_enough_memory(min_kb=min_mem_kb):
                    note = "skipped-low-memory"
                    row = {"variant":"B_recommended","repeat":rep,"cli":best_cli,"energy_j":None,"time_s":None,"timestamp":datetime.now().isoformat(),"notes":note,"raw_cmd":"","raw_stdout":""}
                    writer.writerow(row); rows.append(row); continue
                safe_set_cluster_freq("little", 0)
                safe_set_cluster_freq("big", 0)
                time.sleep(COOL_DOWN_SEC)
                parts = best_cli.strip().split()
                if parts and parts[0].startswith("--"):
                    prog_idx = None
                    for i, tok in enumerate(parts):
                        if tok.startswith("./") or tok.startswith("pipe_") or tok.startswith("farm") or tok.startswith("default_"):
                            prog_idx = i; break
                    prog_cli = " ".join(parts[prog_idx:]) if prog_idx is not None else best_cli
                else:
                    prog_cli = best_cli
                e_b, t_b, raw_cmd_b, raw_out_b = run_real_measurement_program_first(prog_cli, core_mask=rec_mask, little_freq=rec_lf_hz, big_freq=rec_bf_hz)
                note = "ok" if e_b is not None else "failed"
                row = {"variant":"B_recommended","repeat":rep,"cli":prog_cli,"energy_j":e_b,"time_s":t_b,"timestamp":datetime.now().isoformat(),"notes":note,"raw_cmd":raw_cmd_b,"raw_stdout":raw_out_b}
                writer.writerow(row); rows.append(row)
                time.sleep(0.5)
            print("\nSetting default-run frequencies: little=1.5GHz, big=2.0GHz (no taskset).")
            little_freq_hz = 1500000; big_freq_hz = 2000000
            for rep in range(1, repeat_count + 1):
                gc.collect()
                if not has_enough_memory(min_kb=min_mem_kb):
                    note = "skipped-low-memory"
                    row = {"variant":"A_default","repeat":rep,"cli":default_cli_base,"energy_j":None,"time_s":None,"timestamp":datetime.now().isoformat(),"notes":note,"raw_cmd":"","raw_stdout":""}
                    writer.writerow(row); rows.append(row); continue
                safe_set_cluster_freq("little", little_freq_hz)
                safe_set_cluster_freq("big", big_freq_hz)
                time.sleep(COOL_DOWN_SEC)
                e_a, t_a, raw_cmd_a, raw_out_a = run_real_measurement_program_first(default_cli_base, core_mask=None, little_freq=None, big_freq=None)
                note = "ok" if e_a is not None else "failed"
                row = {"variant":"A_default","repeat":rep,"cli":default_cli_base,"energy_j":e_a,"time_s":t_a,"timestamp":datetime.now().isoformat(),"notes":note,"raw_cmd":raw_cmd_a,"raw_stdout":raw_out_a}
                writer.writerow(row); rows.append(row)
                time.sleep(0.5)
        print("\nRestoring CPU governor to 'ondemand' after default runs.")
        try:
            restore_default_governor()
        except Exception:
            print("restore_default_governor failed; continuing.")
        b_entries_all = [r for r in rows if r["variant"]=="B_recommended"]
        a_entries_all = [r for r in rows if r["variant"]=="A_default"]
        b_success = [r for r in b_entries_all if r["energy_j"] is not None and r["time_s"] is not None]
        a_success = [r for r in a_entries_all if r["energy_j"] is not None and r["time_s"] is not None]
        pairs = []
        pair_count = min(len(b_success), len(a_success))
        for i in range(pair_count):
            b = b_success[i]; a = a_success[i]
            pct_e = 100.0 * (b["energy_j"] - a["energy_j"]) / a["energy_j"]
            pct_t = 100.0 * (b["time_s"] - a["time_s"]) / a["time_s"]
            pairs.append({"rep": i+1, "pct_e": pct_e, "pct_t": pct_t})
        with open(report_name, "w") as rf:
            rf.write("Compare Default vs CLI Report\n")
            rf.write(f"Workload: {app} {wl} cnt={cnt} sizes={sizes}\n")
            rf.write(f"Recommended CLI: {best_cli}\n")
            rf.write(f"Default CLI (minimal, no taskset): {default_cli_base}\n\n")
            rf.write(f"Repeats attempted: {repeat_count}\n")
            rf.write(f"Repeats successful - Recommended: {len(b_success)} / {repeat_count}\n")
            rf.write(f"Repeats successful - Default:     {len(a_success)} / {repeat_count}\n\n")
            if pairs:
                pct_es = [p["pct_e"] for p in pairs]; pct_ts = [p["pct_t"] for p in pairs]
                rf.write("Per-pair percent changes (B vs A):\n")
                for p in pairs:
                    rf.write(f"  pair {p['rep']}: energy {p['pct_e']:.2f}% , time {p['pct_t']:.2f}%\n")
                rf.write("\nAggregate:\n")
                rf.write(f"  mean percent energy (B vs A) = {float(np.mean(pct_es)):.2f}%\n")
                rf.write(f"  mean percent time   (B vs A) = {float(np.mean(pct_ts)):.2f}%\n")
                wins_e = sum(1 for v in pct_es if v < 0); wins_t = sum(1 for v in pct_ts if v < 0)
                rf.write(f"  B improved energy in {wins_e}/{len(pct_es)} pairs\n")
                rf.write(f"  B improved time   in {wins_t}/{len(pct_ts)} pairs\n")
            else:
                rf.write("No successful paired measurements to compare.\n")
            rf.write("\nRaw CSV: {csv_name}\n")
        print(f"Wrote comparison CSV → {csv_name}")
        print(f"Wrote comparison report → {report_name}")

        def append_run_summary(
            summary_path, timestamp, app, workload_type, sizes, cnt, objective,
            chosen_cli, chosen_energy, chosen_time, alt_rows, b_success, a_success, pairs,
            csv_name, report_name, notes=""
        ):
            sizes_str = "_".join(str(s) for s in sizes)
            pattern = f"real_vs_real_{app}_{objective}_{workload_type}_{cnt}_{sizes_str}_"
            cand_files = [p for p in os.listdir(".") if p.startswith(pattern) and p.endswith(".txt")]
            cand_files.sort()
            alt_entries = []
            if cand_files:
                latest = cand_files[-1]
                try:
                    with open(latest, "r") as rf:
                        lines = rf.read().splitlines()
                    in_alts = False; cur_cli = None; cur_e = None; cur_t = None
                    for L in lines:
                        s = L.strip()
                        if not in_alts:
                            if s.startswith(">>> Alternatives vs Chosen"):
                                in_alts = True
                            continue
                        if not s: continue
                        if s.startswith("--") or s.startswith("./") or "./pipe" in s or "./farm" in s:
                            if cur_cli is not None:
                                alt_entries.append({"cli": cur_cli, "energy_j": cur_e, "time_s": cur_t})
                            cur_cli = s; cur_e = None; cur_t = None; continue
                        m_e = re.search(r"Energy:\s*([0-9.]+)", s)
                        if m_e:
                            try: cur_e = float(m_e.group(1))
                            except: cur_e = None
                            continue
                        m_t = re.search(r"Time:\s*([0-9.]+)", s)
                        if m_t:
                            try: cur_t = float(m_t.group(1))
                            except: cur_t = None
                            continue
                        if s.startswith(">>>") and cur_cli is not None:
                            alt_entries.append({"cli": cur_cli, "energy_j": cur_e, "time_s": cur_t})
                            cur_cli = None; cur_e = None; cur_t = None; break
                    if cur_cli is not None:
                        alt_entries.append({"cli": cur_cli, "energy_j": cur_e, "time_s": cur_t})
                except Exception:
                    alt_entries = []
            if not alt_entries:
                cli_to_vals = {}
                for r in alt_rows:
                    cli = (r.get("cli") or "").replace("\n", " ").replace("\r", " ")
                    if not cli or r.get("variant") == "A_default": continue
                    e = r.get("energy_j"); t = r.get("time_s")
                    if cli not in cli_to_vals: cli_to_vals[cli] = {"e": [], "t": []}
                    if e is not None:
                        try: cli_to_vals[cli]["e"].append(float(e))
                        except: pass
                    if t is not None:
                        try: cli_to_vals[cli]["t"].append(float(t))
                        except: pass
                for cli, vals in cli_to_vals.items():
                    e_mean = float(np.mean(vals["e"])) if vals["e"] else None
                    t_mean = float(np.mean(vals["t"])) if vals["t"] else None
                    alt_entries.append({"cli": cli, "energy_j": e_mean, "time_s": t_mean})
            alt_chosen_e_pct = []; alt_chosen_t_pct = []
            for ae in alt_entries:
                e = ae.get("energy_j"); t = ae.get("time_s")
                if e is None or e == 0 or chosen_energy is None: alt_chosen_e_pct.append("")
                else:
                    try: alt_chosen_e_pct.append(f"{100.0 * (chosen_energy - e) / e:.3f}")
                    except: alt_chosen_e_pct.append("")
                if t is None or t == 0 or chosen_time is None: alt_chosen_t_pct.append("")
                else:
                    try: alt_chosen_t_pct.append(f"{100.0 * (chosen_time - t) / t:.3f}")
                    except: alt_chosen_t_pct.append("")
            default_energy = None; default_time = None
            if a_success:
                try: default_energy = float(np.mean([r["energy_j"] for r in a_success if r.get("energy_j") is not None]))
                except: default_energy = None
                try: default_time = float(np.mean([r["time_s"] for r in a_success if r.get("time_s") is not None]))
                except: default_time = None
            default_vs_cli_energy_pct = ""; default_vs_cli_time_pct = ""
            if chosen_energy is not None and default_energy is not None and default_energy != 0:
                try: default_vs_cli_energy_pct = f"{100.0 * (chosen_energy - default_energy) / default_energy:.3f}"
                except: default_vs_cli_energy_pct = ""
            if chosen_time is not None and default_time is not None and default_time != 0:
                try: default_vs_cli_time_pct = f"{100.0 * (chosen_time - default_time) / default_time:.3f}"
                except: default_vs_cli_time_pct = ""
            n_alternatives_measured = len(alt_entries)
            n_alternatives_successful = sum(1 for x in alt_entries if x.get("energy_j") is not None and x.get("time_s") is not None)
            n_repeats_recommended = len([r for r in alt_rows if r.get("variant", "").startswith("B_")])
            n_repeats_default = len([r for r in alt_rows if r.get("variant", "").startswith("A_")])
            defaults_successful = len(a_success)
            row = {
                "timestamp": timestamp, "app": app, "workload_type": workload_type,
                "sizes": sizes_str, "cnt": cnt, "objective": objective,
                "chosen_cli": chosen_cli,
                "chosen_energy_j": "" if chosen_energy is None else f"{chosen_energy:.6f}",
                "chosen_time_s": "" if chosen_time is None else f"{chosen_time:.6f}",
            }
            for i, ae in enumerate(alt_entries):
                idx = i + 1
                cli_n = (ae.get("cli") or "").replace(";", "\\;")
                row[f"alt_{idx}"] = cli_n
                row[f"alt_{idx}_energy_j"] = "" if ae.get("energy_j") is None else f"{ae['energy_j']:.6f}"
                row[f"alt_{idx}_time_s"] = "" if ae.get("time_s") is None else f"{ae['time_s']:.6f}"
                row[f"chosen_vs_alt_{idx}_energy_pct"] = alt_chosen_e_pct[i]
                row[f"chosen_vs_alt_{idx}_time_pct"] = alt_chosen_t_pct[i]
            row["default_energy_j"] = "" if default_energy is None else f"{default_energy:.6f}"
            row["default_time_s"] = "" if default_time is None else f"{default_time:.6f}"
            row["default_vs_cli_energy_pct"] = default_vs_cli_energy_pct
            row["default_vs_cli_time_pct"] = default_vs_cli_time_pct
            row.update({
                "n_alternatives_measured": n_alternatives_measured,
                "n_alternatives_successful": n_alternatives_successful,
                "n_repeats_recommended": n_repeats_recommended,
                "n_repeats_default": n_repeats_default,
                "defaults_successful": defaults_successful,
                "notes": notes, "report_file": report_name, "csv_file": csv_name,
            })
            prefix = ["timestamp","app","workload_type","sizes","cnt","objective","chosen_cli","chosen_energy_j","chosen_time_s"]
            alt_cols = []
            for i in range(len(alt_entries)):
                idx = i + 1
                alt_cols += [f"alt_{idx}", f"alt_{idx}_energy_j", f"alt_{idx}_time_s", f"chosen_vs_alt_{idx}_energy_pct", f"chosen_vs_alt_{idx}_time_pct"]
            suffix = ["default_energy_j","default_time_s","default_vs_cli_energy_pct","default_vs_cli_time_pct","n_alternatives_measured","n_alternatives_successful","n_repeats_recommended","n_repeats_default","defaults_successful","notes","report_file","csv_file"]
            fieldnames = prefix + alt_cols + suffix
            new = not os.path.exists(summary_path)
            if new:
                with open(summary_path, "w", newline="") as fsum:
                    writer = csv.DictWriter(fsum, fieldnames=fieldnames)
                    writer.writeheader(); writer.writerow(row)
            else:
                with open(summary_path, "a", newline="") as fsum:
                    writer = csv.DictWriter(fsum, fieldnames=fieldnames)
                    writer.writerow(row)
            print(f"Appended per-alternative run summary to {summary_path}")

        try:
            summary_path = "all_runs_summary.csv"
            append_run_summary(
                summary_path=summary_path,
                timestamp=timestamp,
                app=app,
                workload_type=wl,
                sizes=sizes,
                cnt=cnt,
                objective=obj,
                chosen_cli=best_cli,
                chosen_energy=e_chosen,
                chosen_time=t_chosen,
                alt_rows=rows,
                b_success=b_success,
                a_success=a_success,
                pairs=pairs,
                csv_name=csv_name,
                report_name=report_name,
                notes=""
            )
        except Exception as ex:
            print(f"append_run_summary failed: {ex}")

    try:
        restore_default_governor()
    except Exception:
        pass
    time.sleep(0.2)

if not has_enough_memory(min_kb=min_mem_kb):
    print("Low memory: skipping final governor restore commands.")
else:
    try:
        restore_default_governor()
    except Exception:
        print("final restore_default_governor failed; continuing to exit.")
        time.sleep(COOL_DOWN_SEC)
print("\nRestored CPU governor to 'ondemand' (best-effort)")
sys.exit(0)
