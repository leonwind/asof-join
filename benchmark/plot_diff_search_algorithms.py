import matplotlib.pyplot as plt
import regex as re
import os


from benchmark_plotter import style, texify, colors
texify.latexify(fig_width=3.39, fig_height=1.5)
style.set_custom_style()


class DistributionRun:
    def __init__(self):
        self.strategy_exec_times = {}

    def add_strategy_exec_time(self, strategy, num_positions, exec_time):
        if strategy not in self.strategy_exec_times:
            self.strategy_exec_times[strategy] = []
        self.strategy_exec_times[strategy].append((num_positions, exec_time))

    def __repr__(self):
        return str(self.strategy_exec_times)


def _read_data(path):
    with open(path, 'r') as f:
        data = f.read().splitlines()
    return data


def micro_to_seconds(micro):
    return micro / 1_000_000.0


def micro_to_milli(micro):
    return micro / 1_000.0


def milli_to_seconds(milli):
    return milli / 1_000.0


def plot_diff_search_algos(path):
    raw_data = _read_data(path)
    print(raw_data)

    less_equal = "LE"
    greater_equal = "GE"

    le_data = []
    ge_data = []

    for row in raw_data:
        algo = row.split(" ")[0]
        print(algo)
        time = micro_to_seconds(int(row.split(" ")[2][:-4]))
        print(time)

        if less_equal in row:
            le_data.append((algo, time))
        else:
            ge_data.append((algo, time))

    print(le_data)
    print(ge_data)

    fig, axs = plt.subplots(1, 2)
    bar_labels = ["Binary", "Exponential", "Interpolation"]
    bar_colors = [colors.colors[label] for label in ["blue", "green", "orange"]]

    axs[0].bar(*zip(*le_data), color=bar_colors, label=bar_labels)
    axs[1].bar(*zip(*ge_data), color=bar_colors, label=bar_labels)

    axs[0].set_title("Less Equal ($\leq$)")
    axs[0].set_ylabel("Time [s]")
    axs[0].set_xticks([])
    
    axs[1].set_title("Greater Equal ($\geq$)")
    axs[1].tick_params(axis='y', left=False, labelleft=False)
    #axs[1].set_yticks([])
    axs[1].set_yticklabels([])
    axs[1].set_xticks([])

    handles, labels = axs[0].get_legend_handles_labels()
    print(handles, labels)
    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.56, -0.08),
        ncols=3, frameon=False)

    filename = f"plots/skylake_final/diff_search_algos_side_by_side.pdf"
    print(f"Plotting {filename}")

    plt.tight_layout()

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()



if __name__ == "__main__":
    #interpolation_path = "results/diff_search_algs/interpolation_results.log"
    #exponential_path = "results/diff_search_algs/exponential_search_results.log"
    #binary_path = "results/diff_search_algs/binary_search_results.log"
    #c1.plot_data(interpolation_path, exponential_path, binary_path)

    #c2.plot_search_diff_algos("results/skylake/diff_search_algorithms.log")
    plot_diff_search_algos("results/skylake_final/diff_search_uni.log")
