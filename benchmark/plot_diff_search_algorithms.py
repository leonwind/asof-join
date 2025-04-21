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


class PlotFullExecTimesDiffSearchAlgos:

    def _parse_data_into_groups(data):
        groups = {}
        group_pattern = r'\[(\w+)-(\d+)\.csv\]'

        curr_distribution = None
        curr_num_positions = None

        for row in data:
            if row.startswith("Run"):
                regex_match = re.search(group_pattern, row)
                if regex_match is None:
                    print(f"Regex error: {row}")
                curr_distribution = regex_match.group(1)
                curr_num_positions = int(regex_match.group(2))
                if curr_distribution not in groups:
                    groups[curr_distribution] = DistributionRun()

            else:
                strategy_exec_time = row.split(": ")
                strategy = strategy_exec_time[0]
                exec_time = int(strategy_exec_time[1])
                groups[curr_distribution].add_strategy_exec_time(
                    strategy, curr_num_positions, micro_to_seconds(exec_time))

        return groups


    def _plot_search_algs(interpolation_data, exponential_data, binary_data, dir_name, log_scale=True):
        print(dir_name)
        markers = ['*','o','x','^','s','D']
        print(interpolation_data)
        print(exponential_data)
        print(binary_data)

        line_styles = ["-", "--"]

        i = 0
        for strategy, exec_times in interpolation_data.strategy_exec_times.items():
            exec_times.sort(key = lambda x: x[0])
            plt.plot(*zip(*exec_times), line_styles[i % 2], marker=markers[i], label=f"{strategy.title()} + Interpolation")
            i += 1

        for strategy, exec_times in exponential_data.strategy_exec_times.items():
            exec_times.sort(key = lambda x: x[0])
            plt.plot(*zip(*exec_times), line_styles[i % 2], marker=markers[i], label=f"{strategy.title()} + Exponential")
            i += 1

        for strategy, exec_times in binary_data.strategy_exec_times.items():
            exec_times.sort(key = lambda x: x[0])
            plt.plot(*zip(*exec_times), line_styles[i % 2], marker=markers[i], label=f"{strategy.title()} + Binary")
            i += 1

        if log_scale:
            plt.xscale("log")
            #plt.yscale("log")

        log_label_prefix = "[log]" if log_scale else ""
        plt.xlabel(f"Num positions {log_label_prefix}")
        plt.ylabel(f"Time [s]")

        plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))

        is_log_plot_path = "log_" if log_scale else ""
        filename = f"plots/{dir_name}/{is_log_plot_path}uniform_plot.pdf"
        print(f"Plotting {filename}")

        plt.savefig(filename, dpi=400, bbox_inches="tight")
        os.system(f"pdfcrop {filename} {filename}")

        plt.close()


    def _get_group(file):
        raw = _read_data(file)
        return c1._parse_data_into_groups(raw)


    def plot_data(interpolation_path, exponential_path, binary_path):
        interpolation_groups = c1._get_group(interpolation_path)
        exponential_groups = c1._get_group(exponential_path)
        binary_groups = c1._get_group(binary_path)


        dir_name = interpolation_path.split("/")[1].split(".")[0]

        c1._plot_search_algs(
            interpolation_groups["uniform"],
            exponential_groups["uniform"],
            binary_groups["uniform"],
            dir_name,
            log_scale=True)

c1 = PlotFullExecTimesDiffSearchAlgos


"""
Plot only the time of the search phases of the different algorithms in a bar plot.
Normalize over the number of searches.
"""
class PlotBarPlotsOnlySearchPhaseDiffAlgos:

    def _bar_plot(left_data, right_data):
        fig, axes = plt.subplots(1, 2)

        left_bars = []
        left_bars.append(axes[0].bar("Binary", left_data["Binary"], color=colors.colors["blue"], label="Binary"))
        left_bars.append(axes[0].bar("Exponential", left_data["Exponential"], color=colors.colors["green"], label="Exponential"))
        left_bars.append(axes[0].bar("Interpolation", left_data["Interpolation"], color=colors.colors["orange"], label="Interpolation"))

        axes[0].set_title("Partition Left")
        axes[0].set_ylabel("Time [us]")
        axes[0].set_xticks([])

        right_bars = []
        right_bars.append(axes[1].bar("Binary", right_data["Binary"], color=colors.colors["blue"], label="Binary"))
        right_bars.append(axes[1].bar("Exponential", right_data["Exponential"], color=colors.colors["green"], label="Exponential"))
        right_bars.append(axes[1].bar("Interpolation", right_data["Interpolation"], color=colors.colors["orange"], label="Interpolation"))
        axes[1].set_title("Partition Right")
        #axes[1].set_ylabel("Time [us]$")

        axes[0].set_xticks([])
        axes[1].set_xticks([])

        #axes[0].set_yticks([0, 1, 2])
        #axes[1].set_yticks([0, 1, 2])

        handles, labels = axes[0].get_legend_handles_labels()
        print(handles, labels)
        fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.53, -0.07),
           ncols=3, prop={"size": 8})
            #borderaxespad=0.2, ncols=3, prop={"size": 8})

        filename = f"plots/skylake/diff_search_algos_side_by_side.pdf"
        print(f"Plotting {filename}")

        plt.tight_layout()

        plt.savefig(filename, dpi=400, bbox_inches="tight")
        os.system(f"pdfcrop {filename} {filename}")

        plt.close()


    def plot_search_diff_algos(path):
        raw_data = _read_data(path)
        print(raw_data)

        left_data = {}
        right_data = {}

        left_normalization_constant = 24 / (128 * 2000000)
        right_normalization_constant = 24 / (100000 * 2**10)

        pattern = r"^([A-Za-z]+)\s([A-Za-z]+):\s(\d+)\[us\]$"

        for row in raw_data:
            print(row)
            match = re.search(pattern, row)
            if match is None:
                print("No match")
            else:
                strategy = match.group(1)
                label = match.group(2)
                duration = int(match.group(3))
                print(strategy, label, duration)

                if strategy == "Left":
                    left_data[label] = duration * left_normalization_constant
                else:
                    right_data[label] = duration * right_normalization_constant

        print(left_data)
        print(right_data)

        c2._bar_plot(left_data, right_data)

c2 = PlotBarPlotsOnlySearchPhaseDiffAlgos


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
    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.56, -0.12),
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
