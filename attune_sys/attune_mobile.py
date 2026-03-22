#!/usr/bin/env python3
import os
import sys
import time
import re
import csv
import pickle
import random
import traceback
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

try:
    import numpy as np
except Exception:
    np = None

random.seed(12345)
if np is not None:
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

REPEAT_COUNT = 3
COOL_DOWN_SEC = 5
MIN_MEM_KB = 120_000
K_ALTS = 5
N_CANDIDATES = 3000
GENERATE_ONLY = False

CLUSTER_POLICIES = {"small": "policy0", "medium": "policy4", "large": "policy6"}

PIXEL_SMALL_FREQS  = [300000,574000,738000,930000,1098000,1197000,1328000,1401000,1598000,1704000,1803000]
PIXEL_MEDIUM_FREQS = [400000,553000,696000,799000,910000,1024000,1197000,1328000,1491000,1663000,1836000,1999000,2130000,2253000]
PIXEL_LARGE_FREQS  = [500000,851000,984000,1106000,1277000,1426000,1582000,1745000,1826000,2048000,2188000,2252000,2401000,2507000,2630000,2704000,2802000]

DEFAULT_FREQS = {"small": 1098000, "medium": 1328000, "large": 1582000}

def snap_to_allowed(freq_hz, allowed):
    try:
        arr = list(allowed)
        f = int(round(float(freq_hz)))
        return min(arr, key=lambda v: abs(v - f))
    except Exception:
        return allowed[len(allowed)//2]

def safe_write(path, value):
    try:
        if os.geteuid() == 0:
            with open(path, "w") as f:
                f.write(str(value) + "\n")
            return True
        cmd = f'echo "{value}" | sudo tee {path} > /dev/null'
        subprocess.run(cmd, shell=True, check=True)
        return True
    except Exception:
        return False

def set_cluster_freq_policy(cluster, freq_hz):
    if cluster not in CLUSTER_POLICIES or freq_hz is None:
        return False
    policy = CLUSTER_POLICIES[cluster]
    allowed = PIXEL_SMALL_FREQS if cluster=="small" else (PIXEL_MEDIUM_FREQS if cluster=="medium" else PIXEL_LARGE_FREQS)
    freq_snap = snap_to_allowed(freq_hz, allowed)
    base = f"/sys/devices/system/cpu/cpufreq/{policy}"
    ok1 = safe_write(os.path.join(base, "scaling_min_freq"), freq_snap)
    ok2 = safe_write(os.path.join(base, "scaling_max_freq"), freq_snap)
    if ok1 or ok2:
        print(f"[info] set_cluster_freq_policy({cluster}) -> {freq_snap} Hz")
        return True
    print(f"[warn] Failed to set cluster {cluster} freq to {freq_snap}")
    return False

def restore_cluster_policy(cluster):
    policy = CLUSTER_POLICIES.get(cluster)
    if not policy:
        return False
    base = f"/sys/devices/system/cpu/cpufreq/{policy}"
    try:
        minf = subprocess.check_output(f"cat {base}/cpuinfo_min_freq", shell=True).decode().strip()
        maxf = subprocess.check_output(f"cat {base}/cpuinfo_max_freq", shell=True).decode().strip()
        safe_write(os.path.join(base, "scaling_min_freq"), int(minf))
        safe_write(os.path.join(base, "scaling_max_freq"), int(maxf))
        print(f"[info] Restored policy {policy} to defaults")
        return True
    except Exception:
        print(f"[warn] Could not restore policy {policy}")
        return False

def restore_all_policies():
    for c in CLUSTER_POLICIES:
        restore_cluster_policy(c)

def has_enough_memory(min_kb=MIN_MEM_KB):
    try:
        with open("/proc/meminfo") as f:
            for L in f:
                if L.startswith("MemAvailable:"):
                    return int(L.split()[1]) > min_kb
    except Exception:
        return False

def safe_int(x, default=0):
    try:
        if x is None or x == "":
            return default
        return int(x)
    except Exception:
        return default

def make_core_indices(small_cnt, medium_cnt, large_cnt):
    s = max(0, min(4, safe_int(small_cnt, 0)))
    m = max(0, min(2, safe_int(medium_cnt, 0)))
    l = max(0, min(2, safe_int(large_cnt, 0)))
    indices = list(range(0, s)) + [4 + i for i in range(m)] + [6 + i for i in range(l)]
    return indices

def cores_to_mask_tri(small_on, medium_on, large_on):
    idxs = make_core_indices(small_on, medium_on, large_on)
    if not idxs:
        return None
    if len(idxs) == 1:
        return str(idxs[0])
    runs = []
    start = prev = idxs[0]
    for x in idxs[1:]:
        if x == prev + 1:
            prev = x; continue
        runs.append((start, prev)); start = prev = x
    runs.append((start, prev))
    parts = [f"{a}" if a==b else f"{a}-{b}" for a,b in runs]
    return ",".join(parts)

def load_model_spec(app):
    with open(f"{app}_model_numpy.pkl", "rb") as f:
        spec = pickle.load(f)
    return spec

def load_configs(app):
    cols = Path(f"{app}_columns.txt").read_text().splitlines()
    cidx = {c:i for i,c in enumerate(cols)}
    npz = Path(f"{app}_configs.npz")
    if npz.exists() and np is not None:
        data = np.load(str(npz), mmap_mode="r")
        pool = data["X"]
        print(f"[ok] Loaded {app}_configs.npz (shape={pool.shape})")
    else:
        pool = np.load(f"{app}_configs.npy", allow_pickle=True)
        print(f"[ok] Loaded {app}_configs.npy (shape={getattr(pool,'shape',None)})")
    return pool, cols, cidx

def predict_tree(node, x):
    while not node["leaf"]:
        node = node["left"] if x[node["feature"]] <= node["threshold"] else node["right"]
    return node["value"]

def predict_forest(trees, x):
    if not trees:
        return 0.0
    return sum(predict_tree(t, x) for t in trees) / len(trees)

def score_factory(num_cols, num_means, cat_cols, categories, forests, alpha, beta):
    def score(raw_cat, raw_num):
        Xv = []
        for col, mean in zip(num_cols, num_means):
            v = raw_num.get(col)
            Xv.append(v if isinstance(v, (int, float)) else mean)
        for col, cats in zip(cat_cols, categories):
            v = raw_cat.get(col, "")
            for cat in cats:
                Xv.append(1.0 if v == cat else 0.0)
        e_raw = predict_forest(forests[0], Xv) if forests and len(forests)>0 else 0.0
        t_raw = predict_forest(forests[1], Xv) if forests and len(forests)>1 else 0.0
        return alpha[0]*e_raw + beta[0], alpha[1]*t_raw + beta[1]
    return score

SYNONYMS = {
    "number_of_workers": ["num_workers", "workers", "n_workers"],
    "chunk_size": ["chunk", "worker_chunk"],
    "avg_small_freq": ["avg_little_freq", "avg_small_frequency"],
    "avg_medium_freq": ["avg_medium_frequency"],
    "avg_large_freq": ["avg_big_freq", "avg_large_frequency"],
    "small_cores_on": ["little_cores_on"],
    "large_cores_on": ["big_cores_on"],
}
def find_col(name, cidx):
    if name in cidx:
        return name
    for s in SYNONYMS.get(name, []):
        if s in cidx:
            return s
    return None

def format_cli(cfg, cidx, cnt, wl, app):
    def get_by_name(key, default=None):
        k = find_col(key, cidx)
        if k is None:
            return default
        idx = cidx.get(k)
        try:
            return cfg[idx]
        except Exception:
            return default

    small = safe_int(get_by_name("small_cores_on", 0))
    medium = safe_int(get_by_name("medium_cores_on", 0))
    large = safe_int(get_by_name("large_cores_on", 0))

    sf_hz_raw = safe_int(get_by_name("avg_small_freq", DEFAULT_FREQS["small"]), DEFAULT_FREQS["small"])
    mf_hz_raw = safe_int(get_by_name("avg_medium_freq", DEFAULT_FREQS["medium"]), DEFAULT_FREQS["medium"])
    lf_hz_raw = safe_int(get_by_name("avg_large_freq", DEFAULT_FREQS["large"]), DEFAULT_FREQS["large"])

    sf_hz_clamped = max(300000, min(1800000, sf_hz_raw))
    mf_hz_clamped = max(400000, min(2250000, mf_hz_raw))
    lf_hz_clamped = max(500000, min(2802000, lf_hz_raw))

    sf_snapped = snap_to_allowed(sf_hz_clamped, PIXEL_SMALL_FREQS)
    mf_snapped = snap_to_allowed(mf_hz_clamped, PIXEL_MEDIUM_FREQS)
    lf_snapped = snap_to_allowed(lf_hz_clamped, PIXEL_LARGE_FREQS)

    sf_g = sf_snapped / 1e6; mf_g = mf_snapped / 1e6; lf_g = lf_snapped / 1e6

    if app == "pipeline":
        grouping = safe_int(get_by_name("grouping", 1), 1)
        grouping = max(1, grouping)
        stage_wls = [str(safe_int(get_by_name(f"workload_size_stage{i}", 0), 0)) for i in range(1, cnt+1)]
        if grouping == 1:
            qs = [str(max(1, safe_int(get_by_name(f"queue{i}_size", 1), 1))) for i in range(1, cnt+1)]
            ths = [str(max(1, safe_int(get_by_name(f"stage{i}_threads", 1), 1))) for i in range(1, cnt+1)]
        else:
            first_group_size = 2
            groups = []
            if first_group_size > 0:
                groups.append(list(range(0, min(first_group_size, cnt))))
            idx = min(first_group_size, cnt)
            while idx < cnt:
                groups.append([idx]); idx += 1
            if sum(len(g) for g in groups) != cnt:
                qs = [str(max(1, safe_int(get_by_name(f"queue{i}_size", 1), 1))) for i in range(1, cnt+1)]
                ths = [str(max(1, safe_int(get_by_name(f"stage{i}_threads", 1), 1))) for i in range(1, cnt+1)]
            else:
                qs = []
                ths = []
                for grp in groups:
                    first_idx = grp[0]
                    qs.append(str(max(1, safe_int(get_by_name(f"queue{first_idx+1}_size", 1), 1))))
                    ths.append(str(max(1, safe_int(get_by_name(f"stage{first_idx+1}_threads", 1), 1))))
        tokens = []
        tokens += [f"./pipe_{cnt}stages", wl]
        tokens += stage_wls
        tokens += [str(grouping)]
        tokens += qs
        tokens += ths
        return (
            f"--small-cores {small} --medium-cores {medium} --large-cores {large} "
            f"--small-freq {sf_g:.3f}GHz --medium-freq {mf_g:.3f}GHz --large-freq {lf_g:.3f}GHz "
            + " ".join(tokens)
        )

    workers_raw = get_by_name("number_of_workers", cnt)
    workers = safe_int(workers_raw, cnt)
    workers = max(1, min(workers, 128))
    chunk = safe_int(get_by_name("chunk_size", 1), 1)
    wls = []
    thrs = []
    for i in range(1, workers+1):
        w_val = get_by_name(f"workload_size_w{i}", 0)
        t_val = get_by_name(f"w{i}_threads", 1)
        wls.append(str(safe_int(w_val, 0)))
        thrs.append(str(max(1, safe_int(t_val, 1))))

    if os.environ.get("ATTUNE_DEBUG", ""):
        used = {
            "workers_raw": workers_raw,
            "chunk_raw": chunk,
            "wls": wls,
            "thrs": thrs,
            "small": small, "medium": medium, "large": large,
            "sf_snapped": sf_snapped, "mf_snapped": mf_snapped, "lf_snapped": lf_snapped
        }
        print("DEBUG_FORMAT_CLI_FARM:", used)

    return (
        f"--small-cores {small} --medium-cores {medium} --large-cores {large} "
        f"--small-freq {sf_g:.3f}GHz --medium-freq {mf_g:.3f}GHz --large-freq {lf_g:.3f}GHz "
        f"./farm {workers} {wl} " + " ".join(wls) + f" {chunk} " + " ".join(thrs)
    )

def parse_cli_for_mask_and_freq(cli):
    toks = cli.strip().split()
    s_val = m_val = l_val = None
    sf_hz = mf_hz = lf_hz = None
    for i, tok in enumerate(toks):
        if tok == "--small-cores" and i+1 < len(toks):
            try: s_val = int(toks[i+1])
            except: pass
        if tok == "--medium-cores" and i+1 < len(toks):
            try: m_val = int(toks[i+1])
            except: pass
        if tok == "--large-cores" and i+1 < len(toks):
            try: l_val = int(toks[i+1])
            except: pass
        if tok == "--small-freq" and i+1 < len(toks):
            v = toks[i+1].strip()
            try:
                if v.lower().endswith("ghz"):
                    sf_hz = int(float(v[:-3].strip()) * 1e6)
                else:
                    sf_hz = int(float(v))
            except: sf_hz = None
        if tok == "--medium-freq" and i+1 < len(toks):
            v = toks[i+1].strip()
            try:
                if v.lower().endswith("ghz"):
                    mf_hz = int(float(v[:-3].strip()) * 1e6)
                else:
                    mf_hz = int(float(v))
            except: mf_hz = None
        if tok == "--large-freq" and i+1 < len(toks):
            v = toks[i+1].strip()
            try:
                if v.lower().endswith("ghz"):
                    lf_hz = int(float(v[:-3].strip()) * 1e6)
                else:
                    lf_hz = int(float(v))
            except: lf_hz = None
    mask = cores_to_mask_tri(s_val or 0, m_val or 0, l_val or 0)
    return mask, sf_hz, mf_hz, lf_hz, s_val, m_val, l_val

def random_candidates(pool, cidx, app, wl, cnt, sizes, n):
    if np is not None:
        try:
            mask = (pool[:, cidx["workload_type"]] == wl)
        except Exception:
            mask = np.ones(len(pool), dtype=bool)
        key = "num_stages" if app=="pipeline" else "number_of_workers"
        key_mapped = find_col(key, cidx)
        if key_mapped:
            try:
                mask &= (pool[:, cidx[key_mapped]].astype(int) == cnt)
            except Exception:
                pass
        for i, sz in enumerate(sizes, start=1):
            col = (f"workload_size_stage{i}" if app=="pipeline" else f"workload_size_w{i}")
            col_mapped = find_col(col, cidx)
            if col_mapped:
                try:
                    mask &= (pool[:, cidx[col_mapped]].astype(int) == int(sz))
                except Exception:
                    pass
        real_rows = pool[mask]
        real_count = real_rows.shape[0] if hasattr(real_rows, "shape") else len(real_rows)
        if real_count > 0:
            return [("real", r) for r in real_rows]
        rows = []
        for row in pool:
            cfg = np.array(row, dtype=object, copy=True)
            for i_sz, sz in enumerate(sizes, start=1):
                col = (f"workload_size_stage{i_sz}" if app=="pipeline" else f"workload_size_w{i_sz}")
                col_mapped = find_col(col, cidx)
                if col_mapped:
                    try:
                        cfg[cidx[col_mapped]] = int(sz)
                    except Exception:
                        pass
            rows.append(("rand", cfg))
            if len(rows) >= n: break
        return rows
    else:
        rows = []
        for r in pool:
            rows.append(("real", r))
            if len(rows) >= n: break
        return rows

def recommend_all(pool, cols, cidx, app, wl, cnt, sizes, n_candidates, K,
                  num_cols, num_means, cat_cols, categories, forests, alpha, beta):
    score = score_factory(num_cols, num_means, cat_cols, categories, forests, alpha, beta)
    cand = random_candidates(pool, cidx, app, wl, cnt, sizes, n_candidates)
    reals = [c for c in cand if c[0] == "real"]
    if reals:
        cand = reals
    if not cand:
        return {}, {"energy": [], "time": [], "balanced": []}
    scored = []
    for tag, cfg in cand:
        raw_num = {}
        raw_cat = {}
        for c in num_cols:
            try: raw_num[c] = cfg[cidx[c]]
            except Exception: raw_num[c] = None
        for c in cat_cols:
            try: raw_cat[c] = cfg[cidx[c]]
            except Exception: raw_cat[c] = ""
        e, t = score(raw_cat, raw_num)
        scored.append((tag, cfg, float(e), float(t)))
    E = np.array([s[2] for s in scored]) if scored else np.array([])
    T = np.array([s[3] for s in scored]) if scored else np.array([])
    if E.size == 0 or T.size == 0:
        return {}, {"energy": [], "time": [], "balanced": []}
    n = len(scored)
    front = []
    for i in range(n):
        dominated = False
        for j in range(n):
            if j==i: continue
            if (E[j] <= E[i] and T[j] <= T[i]) and (E[j] < E[i] or T[j] < T[i]):
                dominated = True; break
        if not dominated: front.append(i)
    if not front: front = list(range(n))

    def best_index_for(metric):
        if metric == "energy":
            keyv = E; secv = T
        elif metric == "time":
            keyv = T; secv = E
        else:
            Emax = E.max() if E.size else 1.0; Tmax = T.max() if T.size else 1.0
            keyv = (E / Emax) + (T / Tmax)
            secv = E
        best = None; best_key = None
        for i in front:
            k = float(keyv[i])
            if best is None or k < best_key - 1e-12:
                best, best_key = i, k; continue
            if abs(k - best_key) <= 1e-12:
                if scored[i][0] == "real" and scored[best][0] != "real":
                    best, best_key = i, k; continue
                s_i = float(secv[i]); s_b = float(secv[best])
                if s_i < s_b - 1e-12:
                    best, best_key = i, k; continue
                if abs(s_i - s_b) <= 1e-12 and i < best:
                    best, best_key = i, k
        return best

    results = {}; alts = {}
    for metric in ("energy","time","balanced"):
        bi = best_index_for(metric)
        if bi is None:
            results[metric] = ""
            alts[metric] = []
            continue
        try:
            results[metric] = format_cli(scored[bi][1], cidx, cnt, wl, app)
        except Exception:
            results[metric] = ""
        if metric=="energy": order = np.argsort(E)
        elif metric=="time": order = np.argsort(T)
        else:
            Emax = E.max() if E.size else 1.0; Tmax = T.max() if T.size else 1.0
            order = np.argsort(E/Emax + T/Tmax if (Emax and Tmax) else np.arange(len(E)))
        seen=set(); uniq=[]
        for idx in order:
            if idx==bi: continue
            try:
                cli = format_cli(scored[idx][1], cidx, cnt, wl, app)
            except Exception:
                continue
            if not cli or cli in seen: continue
            seen.add(cli); uniq.append(cli)
            if len(uniq) >= K: break
        alts[metric] = uniq
    return results, alts

def parse_app_output(stdout):
    t=None; e_cpu=None; e_all=None; p_cpu=None; p_all=None
    try:
        m = re.search(r"Elapsed Time:\s*([0-9.,]+)", stdout)
        if m: t = float(m.group(1).replace(",", ""))
        m = re.search(r"Energy\s*\(CPUs only[^\)]*\):\s*([0-9.,]+)", stdout)
        if m: e_cpu = float(m.group(1).replace(",", ""))
        m = re.search(r"Energy\s*\(All channels\):\s*([0-9.,]+)", stdout)
        if m: e_all = float(m.group(1).replace(",", ""))
        m = re.search(r"Power\s*\(CPUs only[^\)]*\):\s*([0-9.,]+)", stdout)
        if m: p_cpu = float(m.group(1).replace(",", ""))
        m = re.search(r"Power\s*\(All channels\):\s*([0-9.,]+)", stdout)
        if m: p_all = float(m.group(1).replace(",", ""))
    except Exception:
        pass
    energy_used=None; used_via=None
    if e_all is not None:
        energy_used=e_all; used_via="all"
    elif e_cpu is not None:
        energy_used=e_cpu; used_via="cpu_only"
    elif p_cpu is not None and t is not None:
        energy_used=p_cpu * t; used_via="cpu_power_computed"
    elif p_all is not None and t is not None:
        energy_used=p_all * t; used_via="all_power_computed"
    return {"elapsed_s":t,"energy_cpu_j":e_cpu,"energy_all_j":e_all,"power_cpu_w":p_cpu,"power_all_w":p_all,"energy_used_j":energy_used,"energy_used_source":used_via,"raw_stdout":stdout}

def run_program_capture(cli_cmd, core_mask=None):
    if not cli_cmd or not cli_cmd.strip(): return None, None, "", ""
    parts = cli_cmd.strip().split()
    prog_idx=None
    for i,tok in enumerate(parts):
        if tok.startswith("./") or tok.startswith("pipe_") or tok.startswith("farm") or tok.startswith("default_"):
            prog_idx=i; break
    prog_cli = " ".join(parts[prog_idx:]) if prog_idx is not None else cli_cmd
    cmd_list = prog_cli.split()
    if core_mask:
        cmd = ["taskset","-c",core_mask] + cmd_list
    else:
        cmd = cmd_list
    raw_cmd = " ".join(cmd)
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
        out = proc.stdout or ""
        parsed = parse_app_output(out)
        return parsed["energy_used_j"], parsed["elapsed_s"], raw_cmd, out
    except Exception:
        return None, None, raw_cmd, traceback.format_exc()

def safe_write_csv_row(path: str, fieldnames: List[str], row: Dict[str, Any]):
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if new:
            writer.writeheader()
        writer.writerow(row)

def append_run_summary(
        summary_path: str, timestamp: str, app: str, workload_type: str, sizes: List[int], cnt: int, objective: str,
        chosen_cli: str, chosen_energy: float, chosen_time: float, alt_rows: List[Dict[str, Any]],
        b_success: List[Dict[str, Any]], a_success: List[Dict[str, Any]], pairs: List[Dict[str, Any]],
        csv_name: str, report_name: str, notes: str = ""
    ):
    sizes_str = "_".join(str(s) for s in sizes)
    pattern = f"real_vs_real_{app}_{objective}_{workload_type}_{cnt}_{sizes_str}_"
    cand_files = [p for p in os.listdir(".") if p.startswith(pattern) and p.endswith(".txt")]
    cand_files.sort()
    alt_entries = []

    for fn in cand_files[::-1]:
        try:
            with open(fn, "r") as fh:
                for L in fh:
                    if L.startswith("MEASURED_ALT_CSV"):
                        parts = L.strip().split(",")
                        cli = None; e = None; t = None
                        for p in parts[1:]:
                            if p.startswith("CLI:"):
                                cli = p[len("CLI:"):].replace("\\;", ";")
                            elif p.startswith("ENERGY_J:"):
                                v = p[len("ENERGY_J:"):]
                                try: e = float(v) if v != "" else None
                                except: e = None
                            elif p.startswith("TIME_S:"):
                                v = p[len("TIME_S:"):]
                                try: t = float(v) if v != "" else None
                                except: t = None
                        if cli:
                            alt_entries.append({"cli": cli, "energy_j": e, "time_s": t})
            if alt_entries:
                break
        except Exception:
            continue

    if not alt_entries and cand_files:
        latest = cand_files[-1]
        try:
            with open(latest, "r") as rf:
                lines = rf.read().splitlines()
            in_alts = False
            cur_cli = None
            cur_e = None
            cur_t = None
            for L in lines:
                s = L.strip()
                if not in_alts:
                    if ">>> Real measurements" in s or "real measurements on alternatives" in s.lower() or "alternatives" in s.lower():
                        in_alts = True
                    continue
                if not s:
                    continue
                if s.startswith("--") or s.startswith("./") or "./pipe" in s or "./farm" in s:
                    if cur_cli is not None:
                        alt_entries.append({"cli": cur_cli, "energy_j": cur_e, "time_s": cur_t})
                    cur_cli = s
                    cur_e = None; cur_t = None
                    continue
                if s.startswith("Raw cmd:"):
                    token = s[len("Raw cmd:"):].strip()
                    if token:
                        if cur_cli is not None:
                            alt_entries.append({"cli": cur_cli, "energy_j": cur_e, "time_s": cur_t})
                        cur_cli = token
                        cur_e = None; cur_t = None
                    continue
                m_both = re.search(r"Real\s+Energy\s*=\s*([0-9eE\+-.]+).*Time\s*=\s*([0-9eE\+-.]+)", s)
                if m_both:
                    try:
                        cur_e = float(m_both.group(1)); cur_t = float(m_both.group(2))
                    except Exception:
                        pass
                    continue
                m_e = re.search(r"(?:Real\s+)?Energy\s*(?:=|:)\s*([0-9eE\+-.]+)", s)
                if m_e:
                    try: cur_e = float(m_e.group(1))
                    except: cur_e = None
                    continue
                m_t = re.search(r"(?:Real\s+)?Time\s*(?:=|:)\s*([0-9eE\+-.]+)", s)
                if m_t:
                    try: cur_t = float(m_t.group(1))
                    except: cur_t = None
                    continue
            if cur_cli is not None:
                alt_entries.append({"cli": cur_cli, "energy_j": cur_e, "time_s": cur_t})
        except Exception:
            alt_entries = []

    if not alt_entries and alt_rows:
        cli_to_vals = {}
        for r in alt_rows:
            cli = (r.get("cli") or "").replace("\n", " ").replace("\r", " ").strip()
            if not cli or r.get("variant", "").startswith("A_"):
                continue
            e = r.get("energy_j"); t = r.get("time_s")
            if cli not in cli_to_vals:
                cli_to_vals[cli] = {"e": [], "t": []}
            if e is not None:
                try: cli_to_vals[cli]["e"].append(float(e))
                except: pass
            if t is not None:
                try: cli_to_vals[cli]["t"].append(float(t))
                except: pass
        for cli, vals in cli_to_vals.items():
            e_mean = float(np.mean(vals["e"])) if (np is not None and vals["e"]) else (sum(vals["e"])/len(vals["e"]) if vals["e"] else None)
            t_mean = float(np.mean(vals["t"])) if (np is not None and vals["t"]) else (sum(vals["t"])/len(vals["t"]) if vals["t"] else None)
            alt_entries.append({"cli": cli, "energy_j": e_mean, "time_s": t_mean})

    alt_chosen_e_pct = []
    alt_chosen_t_pct = []
    for ae in alt_entries:
        e = ae.get("energy_j"); t = ae.get("time_s")
        if e is None or e == 0 or chosen_energy is None:
            alt_chosen_e_pct.append("")
        else:
            try: alt_chosen_e_pct.append(f"{100.0 * (chosen_energy - e) / e:.3f}")
            except: alt_chosen_e_pct.append("")
        if t is None or t == 0 or chosen_time is None:
            alt_chosen_t_pct.append("")
        else:
            try: alt_chosen_t_pct.append(f"{100.0 * (chosen_time - t) / t:.3f}")
            except: alt_chosen_t_pct.append("")

    default_energy = None; default_time = None
    if a_success:
        try: default_energy = float(np.mean([float(r["energy_j"]) for r in a_success if r.get("energy_j") is not None]))
        except Exception: default_energy = None
        try: default_time = float(np.mean([float(r["time_s"]) for r in a_success if r.get("time_s") is not None]))
        except Exception: default_time = None

    default_vs_cli_energy_pct = ""
    default_vs_cli_time_pct = ""
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
    defaults_successful = len(a_success) if a_success else 0

    row = {
        "timestamp": timestamp,
        "app": app,
        "workload_type": workload_type,
        "sizes": sizes_str,
        "cnt": cnt,
        "objective": objective,
        "chosen_cli": (chosen_cli or "").replace("\n", " ").replace("\r", " "),
        "chosen_energy_j": "" if chosen_energy is None else f"{chosen_energy:.6f}",
        "chosen_time_s": "" if chosen_time is None else f"{chosen_time:.6f}",
    }
    MAX_ALTS = 8
    for i, ae in enumerate(alt_entries[:MAX_ALTS]):
        idx = i + 1
        cli_n = (ae.get("cli") or "").replace(";", "\\;").replace("\n", " ").replace("\r", " ")
        row[f"alt_{idx}"] = cli_n
        row[f"alt_{idx}_energy_j"] = "" if ae.get("energy_j") is None else f"{ae['energy_j']:.6f}"
        row[f"alt_{idx}_time_s"] = "" if ae.get("time_s") is None else f"{ae['time_s']:.6f}"
        row[f"chosen_vs_alt_{idx}_energy_pct"] = alt_chosen_e_pct[i]
        row[f"chosen_vs_alt_{idx}_time_pct"] = alt_chosen_t_pct[i]

    row.update({
        "default_energy_j": "" if default_energy is None else f"{default_energy:.6f}",
        "default_time_s": "" if default_time is None else f"{default_time:.6f}",
        "default_vs_cli_energy_pct": default_vs_cli_energy_pct,
        "default_vs_cli_time_pct": default_vs_cli_time_pct,
        "n_alternatives_measured": n_alternatives_measured,
        "n_alternatives_successful": n_alternatives_successful,
        "n_repeats_recommended": n_repeats_recommended,
        "n_repeats_default": n_repeats_default,
        "defaults_successful": defaults_successful,
        "notes": notes,
        "report_file": report_name,
        "csv_file": csv_name
    })

    prefix = ["timestamp", "app", "workload_type", "sizes", "cnt", "objective", "chosen_cli", "chosen_energy_j", "chosen_time_s"]
    alt_cols = []
    for i in range(min(len(alt_entries), MAX_ALTS)):
        idx = i + 1
        alt_cols += [f"alt_{idx}", f"alt_{idx}_energy_j", f"alt_{idx}_time_s", f"chosen_vs_alt_{idx}_energy_pct", f"chosen_vs_alt_{idx}_time_pct"]
    suffix = ["default_energy_j", "default_time_s", "default_vs_cli_energy_pct", "default_vs_cli_time_pct",
              "n_alternatives_measured", "n_alternatives_successful", "n_repeats_recommended", "n_repeats_default",
              "defaults_successful", "notes", "report_file", "csv_file"]
    fieldnames = prefix + alt_cols + suffix

    safe_write_csv_row(summary_path, fieldnames, row)
    print(f"[info] Appended per-run summary to {summary_path}")

def main():
    timestamp_global = datetime.now().strftime("%Y%m%d_%H%M%S")
    for run_idx, run in enumerate(RUNS, start=1):
        app = run.get("app", "pipeline")
        wl = run.get("wl", "cpu_only")
        cnt = int(run.get("cnt", 3))
        sizes = run.get("sizes", [1_000_000]*cnt)
        obj = run.get("obj", "energy")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        print("\n" + "="*70)
        print(f"Run {run_idx}/{len(RUNS)} — app={app} wl={wl} cnt={cnt} sizes={sizes} obj={obj}")
        print("="*70 + "\n")
        try:
            spec = load_model_spec(app)
            pool, cols, cidx = load_configs(app)
        except Exception as ex:
            print(f"[warn] Failed to load model/config for {app}: {ex}", file=sys.stderr)
            traceback.print_exc(); continue

        num_cols = spec.get("num_cols", [])
        num_means = spec.get("num_means", [])
        cat_cols = spec.get("cat_cols", [])
        categories = spec.get("categories", [])
        forests = spec.get("forests", [])
        alpha = spec.get("alpha", [1.0,1.0])
        beta  = spec.get("beta", [0.0,0.0])

        results, alts = recommend_all(pool, cols, cidx, app, wl, cnt, sizes, N_CANDIDATES, K_ALTS,
                                     num_cols, num_means, cat_cols, categories, forests, alpha, beta)
        if not results or obj not in results:
            print("[warn] No recommendation available for this run; skipping.")
            continue
        best_cli = results[obj]
        alt_clis = alts.get(obj, [])
        print("\nRecommended CLI:\n", best_cli)

        if GENERATE_ONLY:
            outf = f"recommended_{app}_{obj}_{wl}_{cnt}_{'_'.join(map(str,sizes))}_{timestamp}.txt"
            with open(outf,"w") as fh:
                fh.write(best_cli + "\n\n")
                for a in alt_clis: fh.write(a + "\n")
            print(f"[ok] Wrote generated CLIs → {outf}")
            continue

        chosen_e = chosen_t = None
        raw_chosen_cmd = raw_chosen_out = ""
        if has_enough_memory(MIN_MEM_KB):
            mask, sf, mf, lf, s_val, m_val, l_val = parse_cli_for_mask_and_freq(best_cli)
            if sf: set_cluster_freq_policy("small", sf)
            if mf: set_cluster_freq_policy("medium", mf)
            if lf: set_cluster_freq_policy("large", lf)
            time.sleep(COOL_DOWN_SEC)
            chosen_e, chosen_t, raw_chosen_cmd, raw_chosen_out = run_program_capture(best_cli, core_mask=mask)
        else:
            print("[warn] Low memory: skipping chosen measurement")

        alt_rows_measured = []
        if alt_clis:
            report_file = f"real_vs_real_{app}_{obj}_{wl}_{cnt}_{'_'.join(map(str,sizes))}_{timestamp}.txt"
            with open(report_file, "w") as rf:
                rf.write(">>> Real measurements on alternatives\n\n")
                rf.write(">>> Chosen config\n"); rf.write(best_cli + "\n\n")
                if raw_chosen_cmd: rf.write("Raw cmd: " + raw_chosen_cmd + "\n")
                if raw_chosen_out: rf.write("\nRaw stdout (chosen):\n" + raw_chosen_out + "\n")
                if chosen_e is not None:
                    rf.write(f"\nChosen real: Energy={chosen_e:.6f} J, Time={chosen_t:.6f} s\n\n")

                for cli in alt_clis:
                    rf.write("----\n"); rf.write(cli + "\n")
                    if not has_enough_memory(MIN_MEM_KB):
                        rf.write("  [warn] skipped (low memory)\n\n")
                        alt_rows_measured.append({"variant":"ALT_skipped","cli":cli,"energy_j":None,"time_s":None})
                        continue
                    mask_a, sfa, mfa, lfa, *_ = parse_cli_for_mask_and_freq(cli)
                    if sfa: set_cluster_freq_policy("small", sfa)
                    if mfa: set_cluster_freq_policy("medium", mfa)
                    if lfa: set_cluster_freq_policy("large", lfa)
                    time.sleep(COOL_DOWN_SEC)
                    e_i, t_i, raw_cmd_i, raw_out_i = run_program_capture(cli, core_mask=mask_a)
                    rf.write("Raw cmd: " + raw_cmd_i + "\n")
                    rf.write("Raw stdout:\n" + (raw_out_i or "") + "\n")
                    if e_i is None:
                        rf.write("  [fail] measurement failed\n\n")
                        alt_rows_measured.append({"variant":"ALT_failed","cli":cli,"energy_j":None,"time_s":None,"raw_cmd":raw_cmd_i,"raw_stdout":raw_out_i})
                        continue
                    if chosen_e is None:
                        de = dt = None
                    else:
                        de = 100.0 * (e_i - chosen_e) / chosen_e if chosen_e else None
                        dt = 100.0 * (t_i - chosen_t) / chosen_t if chosen_t else None
                    rf.write(f"  Real Energy={e_i:.6f} ({de:+.1f}%), Time={t_i:.6f} ({dt:+.1f}%)\n\n")
                    safe_cli = cli.replace(",", ";")
                    rf.write(f"MEASURED_ALT_CSV,CLI:{safe_cli},ENERGY_J:{'' if e_i is None else f'{e_i:.6f}'},TIME_S:{'' if t_i is None else f'{t_i:.6f}'}\n")
                    alt_rows_measured.append({"variant":"ALT_measured","cli":cli,"energy_j":e_i,"time_s":t_i,"raw_cmd":raw_cmd_i,"raw_stdout":raw_out_i})
            print(f"[ok] Wrote real-vs-real report → {report_file}")

        csv_name = f"compare_default_vs_cli_{app}_{obj}_{wl}_{cnt}_{'_'.join(map(str,sizes))}_{timestamp}.csv"
        report_name = f"compare_report_{app}_{obj}_{wl}_{cnt}_{'_'.join(map(str,sizes))}_{timestamp}.txt"
        rows = []
        with open(csv_name, "w", newline="") as csvf:
            fieldnames = ["variant","repeat","cli","energy_j","time_s","timestamp","notes","raw_cmd","raw_stdout"]
            writer = csv.DictWriter(csvf, fieldnames=fieldnames); writer.writeheader()
            rec_mask, rec_sf, rec_mf, rec_lf, *_ = parse_cli_for_mask_and_freq(best_cli)
            for rep in range(1, REPEAT_COUNT+1):
                if not has_enough_memory(MIN_MEM_KB):
                    writer.writerow({"variant":"B_recommended","repeat":rep,"cli":best_cli,"energy_j":None,"time_s":None,"timestamp":datetime.now().isoformat(),"notes":"skipped-low-memory","raw_cmd":"","raw_stdout":""})
                    rows.append({"variant":"B_recommended","energy_j":None,"time_s":None}); continue
                if rec_sf: set_cluster_freq_policy("small", rec_sf)
                if rec_mf: set_cluster_freq_policy("medium", rec_mf)
                if rec_lf: set_cluster_freq_policy("large", rec_lf)
                time.sleep(COOL_DOWN_SEC)
                e_b, t_b, raw_cmd_b, raw_out_b = run_program_capture(best_cli, core_mask=rec_mask)
                note = "ok" if e_b is not None else "failed"
                row = {"variant":"B_recommended","repeat":rep,"cli":best_cli,"energy_j":e_b,"time_s":t_b,"timestamp":datetime.now().isoformat(),"notes":note,"raw_cmd":raw_cmd_b,"raw_stdout":raw_out_b}
                writer.writerow(row); rows.append(row)
                time.sleep(0.5)

            restore_all_policies()
            time.sleep(1.0)
            for rep in range(1, REPEAT_COUNT+1):
                if not has_enough_memory(MIN_MEM_KB):
                    writer.writerow({"variant":"A_default","repeat":rep,"cli":"default","energy_j":None,"time_s":None,"timestamp":datetime.now().isoformat(),"notes":"skipped-low-memory","raw_cmd":"","raw_stdout":""})
                    rows.append({"variant":"A_default","energy_j":None,"time_s":None}); continue
                default_cli = (f"./default_pipe_{cnt}stages {wl} " + " ".join(str(s) for s in sizes)) if app=="pipeline" else (f"./default_farm {cnt} {wl} " + " ".join(str(s) for s in sizes))
                e_a, t_a, raw_cmd_a, raw_out_a = run_program_capture(default_cli, core_mask=None)
                note = "ok" if e_a is not None else "failed"
                row = {"variant":"A_default","repeat":rep,"cli":default_cli,"energy_j":e_a,"time_s":t_a,"timestamp":datetime.now().isoformat(),"notes":note,"raw_cmd":raw_cmd_a,"raw_stdout":raw_out_a}
                writer.writerow(row); rows.append(row)
                time.sleep(0.5)
        restore_all_policies()

        b_entries = [r for r in rows if r["variant"]=="B_recommended" and r["energy_j"] is not None and r["time_s"] is not None]
        a_entries = [r for r in rows if r["variant"]=="A_default" and r["energy_j"] is not None and r["time_s"] is not None]
        pairs = []
        pair_count = min(len(b_entries), len(a_entries))
        for i in range(pair_count):
            b = b_entries[i]; a = a_entries[i]
            pct_e = 100.0 * (b["energy_j"] - a["energy_j"]) / a["energy_j"] if a["energy_j"] else None
            pct_t = 100.0 * (b["time_s"] - a["time_s"]) / a["time_s"] if a["time_s"] else None
            pairs.append({"rep": i+1, "pct_e": pct_e, "pct_t": pct_t})
        with open(report_name, "w") as rf:
            rf.write("Compare Default vs CLI Report\n")
            rf.write(f"Workload: {app} {wl} cnt={cnt} sizes={sizes}\n")
            rf.write(f"Recommended CLI: {best_cli}\n")
            rf.write(f"Default CLI: {default_cli}\n\n")
            rf.write(f"Repeats attempted: {REPEAT_COUNT}\n")
            rf.write(f"Recommended successes: {len(b_entries)}  Default successes: {len(a_entries)}\n\n")
            if pairs:
                for p in pairs:
                    rf.write(f"pair {p['rep']}: energy {p['pct_e']:.2f}% , time {p['pct_t']:.2f}%\n")
                rf.write("\nAggregate:\n")
                rf.write(f"  mean percent energy (B vs A) = {float(np.mean([p['pct_e'] for p in pairs])):.2f}%\n" if np is not None else "")
                rf.write(f"  mean percent time   (B vs A) = {float(np.mean([p['pct_t'] for p in pairs])):.2f}%\n" if np is not None else "")
            else:
                rf.write("No successful paired measurements to compare.\n")
            rf.write(f"\nCSV: {csv_name}\n")
        print(f"\n[ok] Wrote comparison CSV → {csv_name}")
        print(f"[ok] Wrote comparison report → {report_name}")

        try:
            summary_path = "all_runs_summary.csv"
            alt_rows_for_agg = alt_rows_measured if alt_rows_measured else rows
            append_run_summary(
                summary_path=summary_path,
                timestamp=timestamp,
                app=app,
                workload_type=wl,
                sizes=sizes,
                cnt=cnt,
                objective=obj,
                chosen_cli=best_cli,
                chosen_energy=chosen_e,
                chosen_time=chosen_t,
                alt_rows=alt_rows_for_agg,
                b_success=b_entries,
                a_success=a_entries,
                pairs=pairs,
                csv_name=csv_name,
                report_name=report_name,
                notes=""
            )
        except Exception:
            print("[warn] Failed to append run summary:", traceback.format_exc())

        restore_all_policies()
        time.sleep(0.5)

    restore_all_policies()
    print("\n[info] Finished all runs; restored cluster policies (best-effort).")
    sys.exit(0)

if __name__ == "__main__":
    main()

