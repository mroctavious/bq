#!/usr/bin/env python3
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

COLS = ["ts_us","event","job_id","worktime","qid","worker_id","qsize","qweight","extra"]
DISPATCHERS = ["JSQ", "LSL", "LWL", "POWER_OF_2", "POWEROF2", "BAYES_PARTIAL", "BAYESIAN_PARTIAL"]
OUTPUT_DIR = Path(".")
PLOT_SUFFIX = ""


def plot_path(filename):
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    return OUTPUT_DIR / f"{stem}{PLOT_SUFFIX}{suffix}"


def load_log(path):
    """
    Carga un log CSV sin header (o con header accidental) y limpia tipos.
    """
    df = pd.read_csv(
        path,
        names=COLS,
        header=None,
        on_bad_lines="skip"
    )

    # Quitar posible header repetido si existe dentro del archivo
    df = df[df["ts_us"].astype(str) != "ts_us"].copy()

    # Convertir columnas numéricas
    for c in ["ts_us", "job_id", "worktime", "qid", "worker_id", "qsize", "qweight"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # event/extra como string (por seguridad)
    df["event"] = df["event"].astype(str)
    df["extra"] = df["extra"].astype(str)

    # Filtrar filas inválidas
    df = df.dropna(subset=["ts_us", "qid"])
    df = df.sort_values("ts_us", kind="mergesort").reset_index(drop=True)
    return df


def build_queue_events(jsq_df, workers_df):
    """
    Construye una secuencia de deltas por cola:
      - PUSH del dispatcher JSQ => +1 al size
      - POP del worker (tu código lo loguea al terminar) => -1 al size

    OJO: tus qsize/qweight en log vienen del valor ANTERIOR (fetch_add/fetch_sub),
    por eso aquí reconstruimos por deltas y no usamos qsize directamente.
    """
    # Dispatcher: decisiones JSQ
    push = jsq_df[(jsq_df["event"] == "PUSH") & (jsq_df["extra"].isin(DISPATCHERS))].copy()
    push = push[push["job_id"] >= 0].copy()
    push["delta_size"] = 1
    push["delta_weight"] = push["worktime"]

    # Workers: tu evento "POP" ocurre después del sleep => completion/end
    pop = workers_df[(workers_df["extra"] == "WORKER") & (workers_df["event"] == "POP")].copy()
    pop = pop[pop["job_id"] >= 0].copy()  # excluye sentinel -1
    pop["delta_size"] = -1
    pop["delta_weight"] = -pop["worktime"]

    ev = pd.concat([
        push[["ts_us","qid","delta_size","delta_weight"]],
        pop[["ts_us","qid","delta_size","delta_weight"]],
    ], ignore_index=True)

    ev = ev.sort_values("ts_us", kind="mergesort").reset_index(drop=True)
    return ev


def plot_jsq_balance_lines(queue_events, nqueues, sample_every=2000):
    """
    Gráficas simples de balance JSQ:
      1) min/prom/max qsize vs tiempo
      2) spread (max-min) y std(qsize) vs tiempo
    """
    if queue_events.empty:
        print("No hay queue_events para graficar JSQ.")
        return

    sizes = np.zeros(nqueues, dtype=np.int64)

    times = []
    mins = []
    maxs = []
    means = []
    stds = []
    spreads = []

    for i, row in queue_events.iterrows():
        qid = int(row["qid"])
        if 0 <= qid < nqueues:
            sizes[qid] += int(row["delta_size"])
            # Clamp por robustez si hay desfase de logs
            if sizes[qid] < 0:
                sizes[qid] = 0

        if i % sample_every == 0:
            t = row["ts_us"] / 1e6
            mn = sizes.min()
            mx = sizes.max()
            mu = sizes.mean()
            sd = sizes.std()

            times.append(t)
            mins.append(mn)
            maxs.append(mx)
            means.append(mu)
            stds.append(sd)
            spreads.append(mx - mn)

    if not times:
        print("No hay snapshots para plot_jsq_balance_lines.")
        return

    # 1) Min / Prom / Max
    plt.figure(figsize=(12, 5))
    plt.plot(times, mins, label="Min qsize")
    plt.plot(times, means, label="Promedio qsize")
    plt.plot(times, maxs, label="Max qsize")
    plt.xlabel("Tiempo (s)")
    plt.ylabel("Tamaño de cola")
    plt.title("Balance - Min / Promedio / Max de tamaño de cola")
    plt.legend()
    plt.tight_layout()
    plt.savefig(plot_path("jsq_balance_lines.png"), dpi=150)
    plt.close()

    # 2) Spread y std
    plt.figure(figsize=(12, 5))
    plt.plot(times, spreads, label="Spread = max-min")
    plt.plot(times, stds, label="Desv. estándar")
    plt.xlabel("Tiempo (s)")
    plt.ylabel("Desbalance")
    plt.title("Balance - Desbalance entre colas vs tiempo")
    plt.legend()
    plt.tight_layout()
    plt.savefig(plot_path("jsq_balance_spread.png"), dpi=150)
    plt.close()


def plot_workers_throughput(workers_df, bin_ms=100):
    """
    Throughput global (jobs/s) vs tiempo usando bins.
    """
    w = workers_df[
        (workers_df["extra"] == "WORKER") &
        (workers_df["event"] == "POP") &
        (workers_df["job_id"] >= 0)
    ].copy()

    if w.empty:
        print("No hay eventos de worker para throughput.")
        return

    # Tus eventos de worker se registran al terminar el job
    w["t_s"] = w["ts_us"] / 1e6
    bin_s = bin_ms / 1000.0
    w["bin"] = (w["t_s"] / bin_s).astype(int)

    counts = w.groupby("bin")["job_id"].count().sort_index()

    x = counts.index.to_numpy() * bin_s
    y = counts.to_numpy() / bin_s  # jobs/s

    plt.figure(figsize=(12, 5))
    plt.plot(x, y)
    plt.xlabel("Tiempo (s)")
    plt.ylabel("Throughput (jobs/s)")
    plt.title(f"Workers - Throughput global (bins de {bin_ms} ms)")
    plt.tight_layout()
    plt.savefig(plot_path("workers_throughput.png"), dpi=150)
    plt.close()


def plot_workers_bar(workers_df):
    """
    Jobs completados por worker (la gráfica que te gustó).
    """
    w = workers_df[
        (workers_df["extra"] == "WORKER") &
        (workers_df["event"] == "POP") &
        (workers_df["job_id"] >= 0)
    ].copy()

    if w.empty:
        print("No hay eventos de worker para bar chart.")
        return

    counts = w.groupby("worker_id")["job_id"].count().sort_index()

    plt.figure(figsize=(12, 5))
    plt.bar(counts.index.astype(int), counts.values)
    plt.xlabel("Worker ID")
    plt.ylabel("Jobs completados")
    plt.title("Distribución de trabajo por worker")
    plt.tight_layout()
    plt.savefig(plot_path("workers_jobs_bar.png"), dpi=150)
    plt.close()


def plot_dispatch_histogram(jsq_df):
    """
    Histograma de jobs asignados por cola (decisiones del dispatcher JSQ).
    """
    push = jsq_df[
        (jsq_df["event"] == "PUSH") &
        (jsq_df["extra"].isin(DISPATCHERS)) &
        (jsq_df["job_id"] >= 0)
    ].copy()

    if push.empty:
        print("No hay PUSH JSQ para histograma.")
        return

    counts = push.groupby("qid")["job_id"].count().sort_index()

    plt.figure(figsize=(12, 5))
    plt.bar(counts.index.astype(int), counts.values)
    plt.xlabel("Queue ID")
    plt.ylabel("Jobs asignados")
    plt.title("Distribución de jobs asignados por cola")
    plt.tight_layout()
    plt.savefig(plot_path("jsq_dispatch_histogram.png"), dpi=150)
    plt.close()


def main():
    if len(sys.argv) < 4:
        print("Uso: python3 plot_jsq.py <JSQ_simulation.log> <workers_simulation.log> <nqueues> [sample_every] [bin_ms] [output_dir] [suffix]")
        print("Ejemplo: python3 plot_jsq.py default/JSQ_simulation.log default/workers_simulation.log 64 2000 100 default/plots _0")
        sys.exit(1)

    jsq_path = sys.argv[1]
    workers_path = sys.argv[2]
    nqueues = int(sys.argv[3])

    sample_every = int(sys.argv[4]) if len(sys.argv) > 4 else 2000
    bin_ms = int(sys.argv[5]) if len(sys.argv) > 5 else 100
    global OUTPUT_DIR, PLOT_SUFFIX
    OUTPUT_DIR = Path(sys.argv[6]) if len(sys.argv) > 6 else Path(".")
    PLOT_SUFFIX = sys.argv[7] if len(sys.argv) > 7 else ""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    jsq_df = load_log(jsq_path)
    workers_df = load_log(workers_path)

    print(f"JSQ rows loaded: {len(jsq_df)}")
    print(f"Workers rows loaded: {len(workers_df)}")

    # Construir eventos de colas para análisis de balance
    queue_events = build_queue_events(jsq_df, workers_df)
    print(f"Queue events merged: {len(queue_events)}")

    # Graficar
    plot_jsq_balance_lines(queue_events, nqueues=nqueues, sample_every=sample_every)
    plot_workers_throughput(workers_df, bin_ms=bin_ms)
    plot_workers_bar(workers_df)
    plot_dispatch_histogram(jsq_df)

    print("Listo")
    print("Generados:")
    print(" - jsq_balance_lines.png")
    print(" - jsq_balance_spread.png")
    print(" - jsq_dispatch_histogram.png")
    print(" - workers_throughput.png")
    print(" - workers_jobs_bar.png")


if __name__ == "__main__":
    main()
