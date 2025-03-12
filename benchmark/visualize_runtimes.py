import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, Normalize 

import os

from benchmark_plotter import style, texify, colors
texify.latexify(fig_width=3.39)
style.set_custom_style()


# Merge Join
def mj(l, r):
    return l * np.log2(l) + r * np.log2(r) + l + r

# Right partitioning
def rp(l, r):
    return r + r * np.log2(r) + l * np.log2(r)

# Left partitioning
def lp(l, r):
    return l + l * np.log2(l) + r * np.log2(l) + l


def calculate_res_matrix(l_upper, r_upper):
    l_values = np.arange(2, 10000, 500)  # First 99 natural numbers
    r_values = np.arange(2, 10000, 500)

    min_values = np.zeros((len(l_values), len(r_values)))
    cost_values = np.ones((len(l_values), len(r_values)))

    # Calculate the values of the functions and store the minimum for each (l, r)
    for i, l in enumerate(l_values):
        for j, r in enumerate(r_values):
            mj_val = mj(l, r)
            rp_val = rp(l, r)
            lp_val = lp(l, r)
            
            min_values[i, j] = np.argmin([mj_val, rp_val, lp_val])
            
            if lp_val >= rp_val:
                cost_values[i, j] =  lp_val / rp_val
            else:
                cost_values[i, j] =  rp_val / lp_val 

    return min_values, cost_values


def plot():
    min_matrix, cost_matrix = calculate_res_matrix(1000, 1000)
    norm = LogNorm(vmin=np.min(cost_matrix), vmax=np.max(cost_matrix))

    # Plot the heatmap
    fig, ax = plt.subplots()
    cax = ax.matshow(min_matrix, cmap="coolwarm", origin="lower")
    #cax = ax.matshow(cost_matrix, cmap="viridis", norm=norm)


    #for i in range(len(l_values)):
    #    for j in range(len(r_values)):
    #        ax.text(j, i, f"{cost_values[i, j]:.2f}", ha="center", va="center", color="white", fontsize=8)

    cbar = fig.colorbar(cax)
    cbar.set_label("Speedup")

    ax.set_xlabel("Number of Prices")
    ax.set_ylabel("Number of Positions")
    ax.set_title("Heatmap of LP vs RP")

    filename = "plots/theo_runtime.pdf"

    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


if __name__ == "__main__":
    plot()
