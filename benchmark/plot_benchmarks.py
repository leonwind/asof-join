import matplotlib.pyplot as plt
import regex as re
import os

from benchmark_plotter import style, texify, colors
texify.latexify(3.39, 2.5)
style.set_custom_style()

#TOTAL_L1 = L1_SIZE * NUM_CORES
#TOTAL_L2 = L2_SIZE * NUM_CORES
#
#DATA_SIZES = [131, 182, 284, 488, 896, 1712, 3344, 6608, 13136, 26192, 52304, 104528, 208976, 417872, 835664, 1671248, 3342416]


class DistributionRun:
    def __init__(self):
        self.strategy_exec_times = {}

    def add_strategy_exec_time(self, strategy, num_positions, exec_time):
        if strategy not in self.strategy_exec_times:
            self.strategy_exec_times[strategy] = []
        self.strategy_exec_times[strategy].append((num_positions, exec_time))


def _read_data(path):
    with open(path, 'r') as f:
        data = f.read().splitlines()
    return data


def micro_to_seconds(micro):
    return micro / 1_000_000.0


def milli_to_seconds(milli):
    return milli / 1_000.0


def _parse_data_into_groups(data):
    groups = {}
    group_pattern = r'\[([A-Za-z0-9.\-_]+)-(\d+)\]'

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
        
        #else:
        elif row.startswith("PARTITION"):
            strategy_exec_time = row.split(": ")
            strategy = strategy_exec_time[0]
            exec_time = int(strategy_exec_time[1])
            groups[curr_distribution].add_strategy_exec_time(
                strategy, curr_num_positions, micro_to_seconds(exec_time))

    return groups


def _plot_distribution_group(distribution_name, strategy_exec_times, dir_name, log_scale=True):
    markers = ['*','o','x','^','s','D']
    for i, (strategy, exec_times) in enumerate(strategy_exec_times.items()):
        # Sort by num positions
        exec_times.sort(key = lambda x: x[0])
        plt.plot(*zip(*exec_times), marker=markers[i], label=strategy.title())

    if log_scale:
        plt.xscale("log")
        #plt.yscale("log")

    log_label_prefix = "[log]" if log_scale else ""
    plt.xlabel(f"Num positions {log_label_prefix}")
    plt.ylabel(f"Time [s]")

    #plt.title(distribution_name)
    plt.legend(loc="upper left")#, bbox_to_anchor=(1, 0.5))

    #plt.legend(loc="upper center", bbox_to_anchor=(0.5, 1.3),
    #       ncols=3, prop={"size": 8})

    is_log_plot_path = "log_" if log_scale else ""
    filename = f"plots/{dir_name}/{is_log_plot_path}{distribution_name}_plot.pdf"
    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def fix_label(label):
    if label == "Partition Left":
        return "Left Partition Join"
    elif label == "Partition Right":
        return "Right Partition Join"
    elif label ==  "Partition + Copy Left":
        return "Left Partition Join + Copy"
    return label


def _plot_all_distribution_groups(distribution_groups, dir_name):
    markers = ['*','o','x','^','s','D']
    fig, axs = plt.subplots(2, 2, sharex=True, sharey=True)
    axs = axs.flatten()

    plot_idx = 0
    for i, (distribution_name, distribution_group) in enumerate(distribution_groups.items()):
        if distribution_name == "uniform":
            continue

        for j, (strategy, exec_times) in enumerate(distribution_group.strategy_exec_times.items()):
            #if "sorted" in strategy.lower():
            #    continue
            exec_times.sort(key=lambda x: x[0])
            axs[plot_idx].plot(*zip(*exec_times), label=fix_label(strategy.title()))

        axs[plot_idx].set_xscale("log") 
        if plot_idx == 0:
            fig.legend(loc="upper center", ncols=2, bbox_to_anchor=(0.48, 1.09), frameon=False)
        

        if plot_idx < 2:
            axs[plot_idx].set_xticklabels([])
            axs[plot_idx].xaxis.set_ticks_position("none")
        else:
            pass
            #axs[plot_idx].set_xlabel("Left Relation Size [log]")

        if plot_idx % 2 == 0:
            axs[plot_idx].set_ylabel("Time [s]")
        else:
            axs[plot_idx].yaxis.set_ticks_position("none")

        zipf_skew = distribution_name.split("-")[1]
        #title = distribution_name.title().replace("-", " ", 1).replace("_", ".")
        axs[plot_idx].set_title(f"$k = {zipf_skew}$")

        plot_idx += 1

    fig.align_ylabels()
    plt.subplots_adjust(hspace=0.3, wspace=0.25)

    fig.text(0.5, -0.04, f"Left Relation Size [log]", ha='center')

    filename = f"plots/{dir_name}/all_zipf_skews.pdf"
    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def plot_data(path):
    raw_data = _read_data(path)
    groups = _parse_data_into_groups(raw_data)
    dir_name = path.split("/")[1].split(".")[0]

    _plot_all_distribution_groups(groups, dir_name)

    #for distribution_name, distribution_group in groups.items():
    #    _plot_distribution_group(distribution_name,
    #                       distribution_group.strategy_exec_times,
    #                       dir_name,
    #                       log_scale=True)
    #    _plot_distribution_group(distribution_name,
    #                       distribution_group.strategy_exec_times,
    #                       dir_name,
    #                       log_scale=False)
    

if __name__ == "__main__":
    #plot_data("results/zipf_uniform_benchmark.txt")
    #plot_data("results/zipf_large_benchmark.txt")
    #plot_data("results/new_res.txt")
    #plot_data("results/last_results/benchmark.txt")

    #plot_data("results/2024-12-18/benchmark.txt")
    #plot_data("results/2025-01-22/benchmark.txt")

    #plot_data("results/2025-01-26/p90_uniform.log")



    #plot_data("results/copying_vs_locking.log")

    #plot_data("results/2025-02-05/uniform_zipf.log")

    #plot_data("results/skylake/diff_zipf_skews.log")
    #plot_data("results/skylake/uniform_both_sides_phases_breakdown.log")
    plot_data("results/skylake_final/diff_zipf_skews.log")

