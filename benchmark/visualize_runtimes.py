import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, ListedColormap, BoundaryNorm, Normalize, SymLogNorm
from matplotlib.ticker import FuncFormatter


import os

from benchmark_plotter import style, texify, colors
texify.latexify(fig_width=3.39, fig_height=4)
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


def calculate_res_matrix_doubling():
    l_r_end = 100_000_000
    num_values = int(np.log2(l_r_end))

    min_values = np.zeros((num_values, num_values))
    cost_values = np.ones((num_values, num_values))

    i, j = 0, 0
    l, r = 2, 2
    while l < l_r_end:
        while r < l_r_end:
            #print(l, r)
            rp_val = rp(l, r)
            lp_val = lp(l, r)
            
            min_values[i, j] = np.argmin([lp_val, rp_val])
            cost_values[i, j] =  lp_val / rp_val

            j += 1
            r *= 2

        r = 2
        j = 0 
        i += 1
        l *= 2
    
    return min_values, cost_values


def plot_theo_runtime():
    min_matrix, cost_matrix = calculate_res_matrix_doubling()
    return plot_log_matrix(cost_matrix)

    norm = LogNorm(vmin=np.min(cost_matrix), vmax=np.max(cost_matrix))

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


def plot_log_matrix(matrix, l_r_end=100_000_000):
    # Log-scaled tick values
    num_values = int(np.log2(l_r_end))
    tick_values = [2**i for i in range(num_values)]  # Actual l and r values
    tick_labels = [f"{v}" for v in tick_values]

    # Log-transformed matrix (avoid log(0))
    matrix = np.log(matrix)

    # Normalize colors
    vmin, vmax = -abs(matrix).max(), abs(matrix).max()

    # Plot
    fig, ax = plt.subplots()
    cax = ax.imshow(matrix, cmap="coolwarm", origin="lower", norm=Normalize(vmin, vmax))

    # Set log-scale ticks
    ax.set_xticks(np.arange(num_values))
    ax.set_xticklabels(tick_labels, rotation=45, ha="right")
    ax.set_yticks(np.arange(num_values))
    ax.set_yticklabels(tick_labels)

    # Labels
    ax.set_xlabel("r values (log scale)")
    ax.set_ylabel("l values (log scale)")
    ax.set_title("Log Cost Ratio Heatmap")

    # Colorbar
    cbar = plt.colorbar(cax)
    cbar.set_label("log(Cost Ratio)")

    # Save and show
    filename = "plots/tmp.pdf"
    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")
    plt.show()
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

    norm = Normalize(vmin=np.min(matrix), vmax=np.max(matrix))

    # Plot the heatmap
    fig, ax = plt.subplots()
    cax = ax.matshow(matrix, cmap=cmap, norm=norm, origin="lower")
    #cax = ax.matshow(matrix, cmap="coolwarm", origin="lower")
    #cax = ax.matshow(matrix, cmap="seismic", origin="lower", vmin=vmin, vmax=vmax)

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


def parse_res_matrix_log(num_values, data):
    min_values = np.zeros((num_values, num_values))
    cost_values = np.ones((num_values, num_values))

    lp_time, rp_time = None, None
    l_idx, r_idx = None, None

    for row in data:
        if row.startswith("l="):
            if l_idx is not None:
                min_values[l_idx, r_idx] = np.argmin([lp_time, rp_time])
                cost_values[l_idx, r_idx] = lp_time / rp_time

            parts = row.split(", ")
            l = int(parts[0].split("=")[1])
            r = int(parts[1].split("=")[1])
            
            l_idx = int(np.log2(l)) - 1
            r_idx = int(np.log2(r)) - 1
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
    
    return min_values, cost_values


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
    return min_values, cost_values


def plot_actual_runtime(path):
    l_r_max = 100_000_000
    l_r_start = 1_000_000
    l_r_increase = 1_000_000

    num_values = int((l_r_max - l_r_start) / l_r_increase)
    print(f"Num values: {num_values}")

    data = _read_data(path)
    min_matrix, cost_matrix = parse_res_matrix_log(26, data)
    plot_matrix(min_matrix)
    #plot_matrix(cost_matrix)


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


def plot_doubling_all(path):
    num_values = int(np.log2(100_000_000))
    data = _read_data(path)
    actual_min, actual_cost = parse_res_matrix_log(num_values, data)
    theo_min, theo_cost = calculate_res_matrix_doubling()

    plot_4_matrices_square(theo_min, theo_cost, actual_min, actual_cost)


def plot_4_matrices_square(theo_min, theo_cost, actual_min, actual_cost):
    fig, axes = plt.subplots(2, 2, sharex=True, sharey=True)

    log_theo_cost = np.log2(theo_cost)
    log_actual_cost = np.log2(actual_cost)

    vmin = min(log_theo_cost.min(), log_actual_cost.min())
    vmax = max(log_theo_cost.max(), log_actual_cost.max())
    max_abs = max(abs(log_theo_cost).max(), abs(log_actual_cost).max())
    vmin, vmax = -max_abs, max_abs

    #norm = SymLogNorm(linthresh=0.1, linscale=0.5, vmin=vmin, vmax=vmax)
    
    top_left = axes[0, 0].imshow(theo_min, origin="lower", cmap="seismic")
    axes[0, 0].set_title("Theoretical Fastest")

    top_right = axes[0, 1].imshow(actual_min, origin="lower", cmap="seismic")
    axes[0, 1].set_title("Actual Fastest")

    bottom_left = axes[1, 0].imshow(log_theo_cost, origin="lower", cmap="seismic", 
                                    vmin=vmin, vmax=vmax)
    axes[1, 0].set_title("Theoretical Cost")
    
    bottom_right = axes[1, 1].imshow(log_actual_cost, origin="lower", cmap="seismic", 
                                     vmin=vmin, vmax=vmax)
    axes[1, 1].set_title("Actual Cost")

    #axes[1, 0].contour(log_theo_cost)#, levels=10, colors='black', linewidths=0.5)
    #axes[1, 1].contour(log_actual_cost)#, levels=10, colors='black', linewidths=0.5)

    xticks = axes[1, 0].get_xticks()
    yticks = axes[1, 0].get_yticks()

    xtick_labels = [f"$2^{{{int(i)}}}$" for i in xticks]
    ytick_labels = [f"$2^{{{int(i)}}}$" for i in yticks]

    for ax in axes[0, :]:  
        ax.set_xticklabels([])
        ax.xaxis.set_ticks_position('none')
    for ax in axes[1, :]:  
        ax.set_xticklabels(xtick_labels) 
    
    for ax in axes[:, 1]:  
        ax.set_yticklabels([])
        ax.yaxis.set_ticks_position('none')
    for ax in axes[:, 0]:  
        ax.set_yticklabels(ytick_labels)

    fig.text(0.5, 0.17, "Right Relation Size [log]", ha='center')
    fig.text(0.001, 0.55, "Left Relation Size [log]", va='center', rotation=90)

    def positive_label(x, pos):
        actual_val = 2**x
        if actual_val < 1:
            return f"{int(actual_val**(-1))}$\\times$"
        return f"{int(actual_val)}$\\times$"

    cbar = fig.colorbar(bottom_right, ax=axes, orientation="horizontal", fraction=0.033, pad=0.15)
    cbar.ax.xaxis.set_major_formatter(FuncFormatter(positive_label))
    cbar.ax.text(-0.03, 0.5, "LP", va="center", ha="right", transform=cbar.ax.transAxes)
    cbar.ax.text(1.03, 0.5, "RP", va="center", ha="left", transform=cbar.ax.transAxes)

    filename = "plots/skylake/l_vs_r_doubling.pdf"

    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")
    plt.close()



if __name__ == "__main__":
    #plot_theo_runtime()
    #plot_actual_runtime("results/skylake/l_vs_r_doubling.log")
    #plot_theo_and_actual("results/skylake/l_vs_r.log")
    plot_doubling_all("results/skylake/l_vs_r_doubling.log")
