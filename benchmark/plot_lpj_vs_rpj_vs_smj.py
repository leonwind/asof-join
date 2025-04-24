import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, ListedColormap, BoundaryNorm, Normalize, TwoSlopeNorm 
from matplotlib.ticker import FuncFormatter
import matplotlib.colors as mcolors
from matplotlib import cm


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


def positive_label(x, pos):
    actual_val = 2**x
    if actual_val < 1:
        return f"{int(actual_val**(-1))}$\\times$"
    return f"{int(actual_val)}$\\times$"


def calculate_res_matrix_doubling():
    #num_values = int(np.log2(l_r_end))
    num_values = 29
    l_r_end = 2**(num_values - 1)#100_000_000

    min_values = np.zeros((num_values, num_values))
    cost_values = np.ones((num_values, num_values))

    i, j = 0, 0
    l, r = 1, 1
    while l <= l_r_end:
        while r <= l_r_end:
            #print(l, r)
            rp_val = rp(l, r)
            lp_val = lp(l, r)
            
            min_values[i, j] = np.argmin([lp_val, rp_val])
            cost_values[i, j] =  lp_val / rp_val

            j += 1
            r *= 2

        r = 1
        j = 0 
        i += 1
        l *= 2
    
    return min_values, cost_values

def calculate_theo_smj_overhead(num_values):
    l_r_end = 2**(num_values - 1)

    overhead = np.ones((num_values, num_values))

    i, j = 0, 0
    l, r = 1, 1
    while l <= l_r_end:
        while r <= l_r_end:
            #print(l, r)
            rp_val = rp(l, r)
            lp_val = lp(l, r)
            smj_val = mj(l, r)
            
            overhead[i, j] =  smj_val / (min(rp_val, lp_val))

            j += 1
            r *= 2

        r = 1
        j = 0 
        i += 1
        l *= 2
    
    return overhead


def parse_res_matrix_log(num_values, data):
    min_values = np.zeros((num_values, num_values))
    cost_values = np.ones((num_values, num_values))

    lp_time, rp_time = None, None
    l_idx, r_idx = None, None

    for row in data:
        if row.startswith("l="):
            if l_idx is not None:
                min_values[l_idx, r_idx] = np.argmin([lp_time, rp_time]) # use min for smj vs (lpj, rpj)
                cost_values[l_idx, r_idx] = lp_time / rp_time

            parts = row.split(", ")
            l = int(parts[0].split("=")[1])
            r = int(parts[1].split("=")[1])
            
            l_idx = int(np.log2(l))
            r_idx = int(np.log2(r))
            #print(l_idx, r_idx)
        
        elif "LEFT" in row:
            parts = row.split(": ")
            lp_time = int(parts[1])

        elif "RIGHT" in row:
            parts = row.split(": ")
            rp_time = int(parts[1])
    
    # Add last one
    min_values[l_idx, r_idx] = np.argmin([lp_time, rp_time]) # use min for smj vs (lpj, rpj)
    cost_values[l_idx, r_idx] = lp_time / rp_time
    
    return min_values, cost_values


def parse_smj_speedup(num_values, smj_data):
    smj_times = np.full((num_values, num_values), np.nan)

    smj_time = None
    l_idx, r_idx = None, None

    for row in smj_data:
        if row.startswith("l="):
            if l_idx is not None:
                smj_times[l_idx, r_idx] = smj_time
            
            parts = row.split(", ")
            l = int(parts[0].split("=")[1])
            r = int(parts[1].split("=")[1])
            
            l_idx = int(np.log2(l))
            r_idx = int(np.log2(r))
            print(l_idx, r_idx)
        
        else:
            parts = row.split(": ")
            smj_time = int(parts[1])
    smj_times[l_idx, r_idx] = smj_time

    return smj_times


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


def plot_doubling_all(path):
    #num_values = int(np.log2(100_000_000))
    num_values = 29
    data = _read_data(path)
    actual_min, actual_cost = parse_res_matrix_log(num_values, data)
    theo_min, theo_cost = calculate_res_matrix_doubling()

    plot_4_matrices_square(theo_min, theo_cost, actual_min, actual_cost)


def plot_smj_vs_lp_rp(smj_path, p_path):
    num_values = 29
    smj_data = _read_data(smj_path)
    lp_rp_data = _read_data(p_path)
    lp_rp_min, _ = parse_res_matrix_log(num_values, lp_rp_data)
    smj_speed_up = parse_smj_speedup(num_values, smj_data)
    theo_smj_speed_up = calculate_theo_smj_overhead(num_values)
    print(smj_speed_up)
    print(np.sum(smj_speed_up >= 0))
    print(np.sum(smj_speed_up < 0))
    plot_smj_speed_up_and_binary(smj_speed_up / lp_rp_min, theo_smj_speed_up)


def plot_smj_speed_up_and_binary(smj_data, smj_theo):
    speedup = np.log2(smj_data)

    fig, ax = plt.subplots(1, 2, sharex=True, sharey=True)
    axs = ax.flatten()

    vmin = speedup.min()
    vmax = speedup.max()
    print(vmin, vmax)

    abs_max = max(abs(vmin), abs(vmax))
    vmin = -abs_max
    vmax = abs_max
    print(vmin, vmax)

    axs[0].imshow(smj_theo, origin="lower", cmap="seismic",
                 vmin=vmin, vmax=vmax)
    axs[0].set_title("Theoretical Overhead", fontsize=10)

    seismic_red = mcolors.LinearSegmentedColormap.from_list(
        "seismic_red_only", cm.seismic(np.linspace(0.5, 1.0, 256))
    )

    actual_overhead = axs[1].imshow(speedup, origin="lower", cmap=seismic_red,
                 vmin=0, vmax=vmax)
    axs[1].set_title("Actual Overhead", fontsize=10)

    axs[1].text(0.1, 0.9, 'B1',
        color="white",
        horizontalalignment='center',
        verticalalignment='center',
        transform = axs[1].transAxes)
    
    axs[1].text(0.9, 0.1, 'B2',
        color="white",
        horizontalalignment='center',
        verticalalignment='center',
        transform = axs[1].transAxes)
    
    axs[1].text(0.5, 0.5, 'B3',
        color="black",
        horizontalalignment='center',
        verticalalignment='center',
        transform = axs[1].transAxes)
    
    axs[1].text(0.9, 0.9, 'B4',
        color="black",
        horizontalalignment='center',
        verticalalignment='center',
        transform = axs[1].transAxes)


    all_powers_of_10 = [0, 3.32, 6.64, 9.97, 13.29, 16.61, 19.93, 23.25, 26.57]
    all_labels = [r"$10^{{{}}}$".format(i) for i in range(len(all_powers_of_10))]
    
    powers_of_10 = all_powers_of_10[::2]
    labels = all_labels[::2]

    for ax in axs:
        ax.set_xticks(powers_of_10)
        ax.set_yticks(powers_of_10)
        ax.set_xticklabels(labels)

    axs[1].set_yticklabels([])
    axs[1].yaxis.set_ticks_position('none')
    axs[0].set_yticklabels(labels) 

    axs[0].set_ylabel("Left Relation Size [log]")
    fig.text(0.5, 0.16, "Right Relation Size [log]", ha='center')

    cbar = fig.colorbar(actual_overhead, ax=axs, orientation="horizontal", fraction=0.033, pad=0.15)
    
    ticks = [0, 2, 4, 5.908]
    cbar.set_ticks(ticks)
    cbar.ax.xaxis.set_major_formatter(FuncFormatter(positive_label))
    cbar.set_label("SMJ Overhead")


    filename = "plots/skylake_final/smj_l_vs_r.pdf"

    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")
    plt.close()


def plot_4_matrices_square(theo_min, theo_cost, actual_min, actual_cost):
    fig, axes = plt.subplots(2, 2, sharex=True, sharey=True)

    log_theo_cost = np.log2(theo_cost)
    log_actual_cost = np.log2(actual_cost)

    vmin = min(log_theo_cost.min(), log_actual_cost.min())
    vmax = max(log_theo_cost.max(), log_actual_cost.max())

    vmin = min(abs(log_theo_cost).min(), abs(log_actual_cost).min())
    max_abs = max(abs(log_theo_cost).max(), abs(log_actual_cost).max())

    max_abs = abs(log_actual_cost.max())

    vmax = max_abs
    vmin, vmax = -max_abs, max_abs


    #norm = SymLogNorm(linthresh=0.1, linscale=0.5, vmin=vmin, vmax=vmax)
    
    top_left = axes[0, 0].imshow(theo_min, origin="lower", cmap="seismic",
                                 vmin=-0.2, vmax=1.2)
    axes[0, 0].set_title("Theoretical Fastest", fontsize=10)

    top_right = axes[0, 1].imshow(actual_min, origin="lower", cmap="seismic",
                                  vmin=-0.25, vmax=1.25)
    axes[0, 1].set_title("Actual Fastest", fontsize=10)

    bottom_left = axes[1, 0].imshow(log_theo_cost, origin="lower", cmap="seismic", 
                                    vmin=vmin, vmax=vmax)
    axes[1, 0].set_title("Theoretical Speed Up", fontsize=10)
    
    bottom_right = axes[1, 1].imshow(log_actual_cost, origin="lower", cmap="seismic", 
                                     vmin=vmin, vmax=vmax)
    axes[1, 1].set_title("Actual Speed Up", fontsize=10)

    axes[1, 1].text(0.1, 0.9, 'A1',
        color="white",
        horizontalalignment='center',
        verticalalignment='center',
        transform = axes[1,1].transAxes)
    
    axes[1, 1].text(0.9, 0.1, 'A2',
        color="white",
        horizontalalignment='center',
        verticalalignment='center',
        transform = axes[1,1].transAxes)
    
    axes[1, 1].text(0.5, 0.5, 'A3',
        color="black",
        horizontalalignment='center',
        verticalalignment='center',
        transform = axes[1,1].transAxes)
    
    axes[1, 1].text(0.9, 0.9, 'A4',
        color="black",
        horizontalalignment='center',
        verticalalignment='center',
        transform = axes[1,1].transAxes)

    #axes[1, 0].contour(log_theo_cost)#, levels=10, colors='black', linewidths=0.5)
    #axes[1, 1].contour(log_actual_cost)#, levels=10, colors='black', linewidths=0.5)

    xticks = axes[1, 0].get_xticks()
    yticks = axes[1, 0].get_yticks()
    print(xticks, yticks)

    xtick_labels = [f"$2^{{{int(i)}}}$" for i in xticks]
    ytick_labels = [f"$2^{{{int(i)}}}$" for i in yticks]

    all_powers_of_10 = [0, 3.32, 6.64, 9.97, 13.29, 16.61, 19.93, 23.25, 26.57]
    all_labels = [r"$10^{{{}}}$".format(i) for i in range(len(all_powers_of_10))]
    
    powers_of_10 = all_powers_of_10[::2]
    labels = all_labels[::2]

    print(xtick_labels, ytick_labels)

    for ax in axes[0, :]:  
        ax.set_xticks(powers_of_10) #
        ax.set_yticks(powers_of_10) #

        ax.set_xticklabels([])
        ax.xaxis.set_ticks_position('none')
    for ax in axes[1, :]:  
        #ax.set_xticklabels(xtick_labels) 
        ax.set_xticklabels(labels) 
    
    for ax in axes[:, 1]:  
        ax.set_xticks(powers_of_10) #
        ax.set_yticks(powers_of_10) #

        ax.set_yticklabels([])
        ax.yaxis.set_ticks_position('none')
    for ax in axes[:, 0]:  
        #ax.set_yticklabels(ytick_labels)
        ax.set_yticklabels(labels)

    fig.text(0.5, 0.17, "Right Relation Size [log]", ha='center')
    fig.text(0.001, 0.55, "Left Relation Size [log]", va='center', rotation=90)

    cbar = fig.colorbar(bottom_right, ax=axes, orientation="horizontal", fraction=0.033, pad=0.15)
    cbar.ax.xaxis.set_major_formatter(FuncFormatter(positive_label))
    cbar.ax.text(-0.03, 0.5, "LPJ", va="center", ha="right", transform=cbar.ax.transAxes)
    cbar.ax.text(1.02, 0.5, "RPJ", va="center", ha="left", transform=cbar.ax.transAxes)

    filename = "plots/skylake_final/l_vs_r_doubling.pdf"

    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")
    plt.close()


if __name__ == "__main__":
    plot_doubling_all("results/skylake_final/l_vs_r.log")

    #plot_smj_vs_lp_rp(
    #    "results/skylake_final/smj/smj_all.log",
    #    "results/skylake_final/l_vs_r.log"
    #)
