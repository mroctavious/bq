#!/usr/bin/env python3
import sys
from pathlib import Path

import matplotlib.image as mpimg
import matplotlib.pyplot as plt


ALGORITHMS = ["JSQ", "LSL", "POWER_OF_2", "BAYES_PARTIAL"]
PLOT_GROUPS = [
    ("jsq_balance_lines", "combined_balance_lines"),
    ("jsq_balance_spread", "combined_balance_spread"),
    ("jsq_dispatch_histogram", "combined_dispatch_histogram"),
    ("workers_throughput", "combined_workers_throughput"),
    ("workers_jobs_bar", "combined_workers_jobs_bar"),
]


def combine_group(results_dir, test_id, source_prefix, output_prefix):
    paths = [
        results_dir / f"{source_prefix}_{algorithm}_{test_id}.png"
        for algorithm in ALGORITHMS
    ]

    if not all(path.exists() for path in paths):
        missing = [str(path) for path in paths if not path.exists()]
        print(f"Skipping {source_prefix}: missing {missing}")
        return

    fig, axes = plt.subplots(2, 2, figsize=(20, 10))

    for axis, algorithm, path in zip(axes.flat, ALGORITHMS, paths):
        axis.imshow(mpimg.imread(path))
        axis.set_title(algorithm)
        axis.axis("off")

    fig.tight_layout()
    out_path = results_dir / f"{output_prefix}_{test_id}.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Generated {out_path}")


def main():
    if len(sys.argv) < 2:
        print("Uso: python3 combine_plots.py <results_dir> [test_id]")
        print("Ejemplo: python3 combine_plots.py results/302 302")
        sys.exit(1)

    results_dir = Path(sys.argv[1])
    test_id = sys.argv[2] if len(sys.argv) > 2 else results_dir.name

    for source_prefix, output_prefix in PLOT_GROUPS:
        combine_group(results_dir, test_id, source_prefix, output_prefix)


if __name__ == "__main__":
    main()
