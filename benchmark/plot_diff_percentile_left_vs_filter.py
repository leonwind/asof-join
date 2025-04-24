import matplotlib.pyplot as plt
import regex as re
import os

from benchmark_plotter import style, texify, colors
texify.latexify(3.39, 1.7)
style.set_custom_style()



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
    #group_pattern = r'\[(\w+)\.csv\]'
    group_pattern = r'Percentile: ([\d\.]+)'

    curr_percentile = None

    for row in data:
        if row.startswith("Percentile"):
            print(row)
            regex_match = re.search(group_pattern, row)
            if regex_match is None:
                print(f"Regex error: {row}")
            curr_percentile = float(regex_match.group(1))
            print(curr_percentile)

        else:
            strategy_exec_time = row.split(": ")
            strategy = strategy_exec_time[0]
            exec_time = int(strategy_exec_time[1])

            if strategy not in groups:
                groups[strategy] = []
            groups[strategy].append((curr_percentile, micro_to_seconds(exec_time)))

    return groups


def fix_label(label):
    if label == "Partition Left":
        return "Left Partition Join"
    elif label == "Partition Right":
        return "Right Partition Join"
    elif label ==  "Partition + Copy Left":
        return "Left Partition Join + Copy"
    elif label == "Partition Left + Filter Min":
        return "Left Partition + Filter"
    return label

def _plot_distribution_group(strategy_exec_times, dir_name, log_scale=True):
    markers = ['*','o','x','^','s','D']
    cs=[colors.colors["blue"], colors.colors["orange"], colors.colors["green"]]

    fig, axes = plt.subplots(1, 2)

    for i, (strategy, exec_times) in enumerate(strategy_exec_times.items()):
        # Sort by percentile
        exec_times.sort(key = lambda x: x[0])
        percentiles, times = zip(*exec_times)
        axes[0].plot(percentiles, times, label=fix_label(strategy.title()))
        axes[0].set_xticks([0, 0.5, 1])

    for i, (strategy, exec_times) in enumerate(strategy_exec_times.items()):
        if "right" in strategy.lower():
            continue

        exec_times.sort(key = lambda x: x[0])
        percentiles, times = zip(*exec_times)
        ratio_times = [times[0] / times[i] for i in range(len(times))]

        axes[1].plot(percentiles, ratio_times, color=cs[i],
                    label='_nolegend_')
        axes[1].set_xticks([0, 0.5, 1])

        #if "filter" in strategy.lower():
        #    theo_times = [ratio_times[0] / (1 - percentiles[i]) for i in range(len(percentiles))]
        #    axes[1].plot(percentiles, theo_times)
    
    #axes[1].set_yscale("log")

    if log_scale:
        axes[0].xscale("log")
        #plt.yscale("log")

    log_label_prefix = "[log]" if log_scale else ""
    #axes[0].set_xlabel(f"Percentile {log_label_prefix}")
    #axes[1].set_xlabel(f"Percentile {log_label_prefix}")
    axes[0].set_ylabel(f"Time [s] {log_label_prefix}")
    axes[1].set_ylabel("Speed Up")

    fig.text(0.57, 0, f"Percentile $p$ {log_label_prefix}", ha='center')

    #plt.title(dir_name.replace("_", " ").title())

    handles, labels = axes[0].get_legend_handles_labels()
    print(handles, labels)
    order = [0,2,1]
    #fig.legend([handles[idx] for idx in order],[labels[idx] for idx in order], 
    #            loc="upper center", bbox_to_anchor=(0.5, 1.225))
    
    fig.legend(
        [handles[idx] for idx in order],
        [labels[idx] for idx in order],
        loc="upper center",
        bbox_to_anchor=(0.54, 1.15),
        ncols=2,
        frameon=False
    )


    #fig.legend(loc="upper center", bbox_to_anchor=(0.52, 1.225), ncols=2)
    
    plt.tight_layout()
    is_log_plot_path = "log_" if log_scale else ""
    filename = f"plots/{dir_name}/{is_log_plot_path}{dir_name}_diff_percentile_plot.pdf"
    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def plot_data(path):
    raw_data = _read_data(path)
    groups = _parse_data_into_groups(raw_data)
    dir_name = path.split("/")[1].split(".")[0]

    print(groups)
    print(dir_name)

    _plot_distribution_group(groups,
                        dir_name,
                        log_scale=False)
    #_plot_distribution_group(groups,
    #                    dir_name,
    #                    log_scale=False)
 

if __name__ == "__main__":
    #plot_data("results/filter_diff_percentile.log")
    plot_data("results/skylake/diff_percentile.log")
