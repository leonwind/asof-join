import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, ListedColormap, BoundaryNorm

import os

from benchmark_plotter import style, texify, colors
texify.latexify(fig_width=3.39)
style.set_custom_style()


def _read_data(path):
    with open(path, 'r') as f:
        data = f.read().splitlines()
    return data


# Merge Join
def mj(l, r):
    return l * np.log2(l) + r * np.log2(r) + l + r

# Right partitioning
def rp(l, r):
    return r + r * np.log2(r) + l * np.log2(r)

# Left partitioning
def lp(l, r):
    return l + l * np.log2(l) + r * np.log2(l) + l


def calculate_res_matrix():
    l_r_start = 1_000_000
    l_r_end = 100_000_000
    l_r_increase = 1_000_000
    l_values = np.arange(l_r_start, l_r_end, l_r_increase)
    r_values = np.arange(l_r_start, l_r_end, l_r_increase)

    min_values = np.zeros((len(l_values), len(r_values)))
    cost_values = np.ones((len(l_values), len(r_values)))

    for i, l in enumerate(l_values):
        for j, r in enumerate(r_values):
            rp_val = rp(l, r)
            lp_val = lp(l, r)
            
            min_values[i, j] = np.argmin([lp_val, rp_val])
            
            cost_values[i, j] =  lp_val / rp_val
            #if lp_val >= rp_val:
            #    cost_values[i, j] =  lp_val / rp_val
            #else:
            #    cost_values[i, j] =  rp_val / lp_val

    return min_values, cost_values


def plot_theo_runtime():
    min_matrix, cost_matrix = calculate_res_matrix()
    #norm = LogNorm(vmin=np.min(cost_matrix), vmax=np.max(cost_matrix))

    cmap = ListedColormap([colors.colors["orange"], colors.colors["blue"]])
    bounds = [-0.5, 0.5, 1.5]
    norm = BoundaryNorm(bounds, cmap.N)

    # Plot the heatmap
    fig, ax = plt.subplots()
    cax = ax.matshow(min_matrix, cmap=cmap, norm=norm, origin="lower")
    #cax = ax.matshow(cost_matrix, cmap="viridis", norm=norm)

    #for i in range(len(l_values)):
    #    for j in range(len(r_values)):
    #        ax.text(j, i, f"{cost_values[i, j]:.2f}", ha="center", va="center", color="white", fontsize=8)

    cbar = fig.colorbar(cax, ticks=[0, 1])
    cbar.ax.set_yticklabels(["LP", "RP"])

    ax.set_xlabel("Number of Prices")
    ax.set_ylabel("Number of Positions")
    #ax.set_title("Heatmap of LP vs RP")

    filename = "plots/theo_runtime.pdf"

    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")
    plt.close()


def plot_matrix(matrix):
    cmap = ListedColormap([colors.colors["orange"], colors.colors["blue"]])
    bounds = [-0.5, 0.5, 1.5]
    norm = BoundaryNorm(bounds, cmap.N)
    matrix = np.log(matrix)

    vmin = matrix.min()
    vmax = matrix.max()
    max_abs = abs(matrix).max()
    vmin, vmax = -max_abs, max_abs

    #norm = Normalize(vmin=np.min(matrix), vmax=np.max(matrix))

    # Plot the heatmap
    fig, ax = plt.subplots()
    #cax = ax.matshow(matrix, cmap=cmap, norm=norm, origin="lower")
    #cax = ax.matshow(matrix, cmap="coolwarm", origin="lower")
    cax = ax.matshow(matrix, cmap="seismic", origin="lower", vmin=vmin, vmax=vmax)

    #for i in range(len(l_values)):
    #    for j in range(len(r_values)):
    #        ax.text(j, i, f"{cost_values[i, j]:.2f}", ha="center", va="center", color="white", fontsize=8)
    #cbar = fig.colorbar(cax, ticks=[0, 1])
    cbar = plt.colorbar(cax)

    #cbar.ax.set_yticklabels(["LP", "RP"])
    #cbar.set_label("Preferred Partitioning")

    ax.set_xlabel("Number of Prices")
    ax.set_ylabel("Number of Positions")
    #ax.set_title("Heatmap of LP vs RP")

    filename = "plots/tmp.pdf"

    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")
    plt.close()


def parse_res_matrix(num_values, data):
    l_r_start = 1_000_000
    l_r_increase = 1_000_000

    min_values = np.zeros((num_values, num_values))
    cost_values = np.ones((num_values, num_values))

    lp_time, rp_time = None, None
    l_idx, r_idx = None, None

    for row in data:
        if row.startswith("l="):
            if l_idx is not None:
                min_values[l_idx, r_idx] = np.argmin([lp_time, rp_time])
                cost_values[l_idx, r_idx] = lp_time / rp_time
                #if lp_time >= rp_time:
                #    cost_values[l_idx, r_idx] = lp_time / rp_time
                #else:
                #    cost_values[l_idx, r_idx] = rp_time / lp_time

            parts = row.split(", ")
            l = int(parts[0].split("=")[1])
            r = int(parts[1].split("=")[1])
            
            l_idx = (l - l_r_start) // l_r_increase 
            r_idx = (r - l_r_start) // l_r_increase

            #print(l_idx, r_idx)
        
        elif "LEFT" in row:
            parts = row.split(": ")
            lp_time = int(parts[1])

        elif "RIGHT" in row:
            parts = row.split(": ")
            rp_time = int(parts[1])
    
    # Add last one
    min_values[l_idx, r_idx] = np.argmin([lp_time, rp_time])
    cost_values[l_idx, r_idx] = lp_time / rp_time
    #if lp_time >= rp_time:
    #    cost_values[l_idx, r_idx] = lp_time / rp_time
    #else:
    #    cost_values[l_idx, r_idx] = rp_time / lp_time

    return min_values, cost_values


def plot_actual_runtime(path):
    l_r_max = 100_000_000
    l_r_start = 1_000_000
    l_r_increase = 1_000_000

    num_values = int((l_r_max - l_r_start) / l_r_increase)
    print(f"Num values: {num_values}")

    data = _read_data(path)
    min_matrix, cost_matrix = parse_res_matrix(num_values, data)
    #plot_matrix(min_matrix)
    plot_matrix(cost_matrix)


def plot_theo_and_actual(actual_path):
    l_r_max = 100_000_000
    l_r_start = 1_000_000
    l_r_increase = 1_000_000

    num_values = int((l_r_max - l_r_start) / l_r_increase)
    print(f"Num values: {num_values}")

    actual_data = _read_data(actual_path)
    actual_min_matrix, actual_cost_matrix = parse_res_matrix(num_values, actual_data)
    theo_min_matrix, theo_cost_matrix = calculate_res_matrix()

    log_actual = np.log2(actual_cost_matrix)
    #log_actual = np.log2(np.maximum(actual_cost_matrix, np.finfo(float).eps))

    log_theo = np.log2(theo_cost_matrix)
    #log_theo = np.log2(np.maximum(theo_cost_matrix, np.finfo(float).eps))

    vmin = min(log_actual.min(), log_theo.min())
    vmax = max(log_actual.max(), log_theo.max())
    max_abs = max(abs(log_actual).max(), abs(log_theo).max())
    vmin, vmax = -max_abs, max_abs

    fig, axes = plt.subplots(1, 2, sharex=True, sharey=True)

    cax1 = axes[0].matshow(log_theo, cmap="seismic", origin="lower")#, vmin=vmin, vmax=vmax)
    axes[0].set_title("Theoretical Runtime")

    cax2 = axes[1].matshow(log_actual, cmap="seismic", origin="lower")#, vmin=vmin, vmax=vmax)
    axes[1].set_title("Actual Runtime")

    for ax in axes:
        ax.set_xlabel("Num of Prices ($r$)")
    axes[0].set_ylabel("Num of Positions ($l$)")

    cbar = fig.colorbar(cax1, ax=axes, orientation="horizontal", fraction=0.065, pad=0.2)
    cbar.set_ticks([])
    cbar.ax.text(-0.03, 0.5, "LP", va="center", ha="right", transform=cbar.ax.transAxes)
    cbar.ax.text(1.03, 0.5, "RP", va="center", ha="left", transform=cbar.ax.transAxes)

    filename = "plots/skylake/l_vs_r.pdf"

    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")
    plt.close()


if __name__ == "__main__":
    #plot_theo_runtime()
    plot_actual_runtime("results/skylake/l_vs_r.log")
    plot_theo_and_actual("results/skylake/l_vs_r.log")
