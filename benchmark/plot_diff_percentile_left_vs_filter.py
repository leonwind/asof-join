import matplotlib.pyplot as plt
import regex as re
import os

from benchmark_plotter import style, texify, colors
texify.latexify(5, 1.8)
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
    group_pattern = r'\[(\w+)\.csv\]'

    curr_percentile = None

    for row in data:
        if row.startswith("Run"):
            regex_match = re.search(group_pattern, row)
            if regex_match is None:
                print(f"Regex error: {row}")
            curr_percentile = int(regex_match.group(1)[1:])
            print(curr_percentile)

        else:
            strategy_exec_time = row.split(": ")
            strategy = strategy_exec_time[0]
            exec_time = int(strategy_exec_time[1])

            if strategy not in groups:
                groups[strategy] = []
            groups[strategy].append((curr_percentile, micro_to_seconds(exec_time)))

    return groups


def _plot_distribution_group(strategy_exec_times, dir_name, log_scale=True):
    markers = ['*','o','x','^','s','D']
    for i, (strategy, exec_times) in enumerate(strategy_exec_times.items()):
        # Sort by percentile
        exec_times.sort(key = lambda x: x[0])
        plt.plot(*zip(*exec_times), marker=markers[i], label=strategy.title())

    if log_scale:
        plt.xscale("log")
        #plt.yscale("log")

    log_label_prefix = "[log]" if log_scale else ""
    plt.xlabel(f"Percentile {log_label_prefix}")
    plt.ylabel(f"Time [s] {log_label_prefix}")

    plt.title(dir_name.replace("_", " ").title())
    plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))

    is_log_plot_path = "log_" if log_scale else ""
    filename = f"plots/{dir_name}/{is_log_plot_path}{dir_name}_plot.pdf"
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
                        log_scale=True)
    _plot_distribution_group(groups,
                        dir_name,
                        log_scale=False)
 

if __name__ == "__main__":
    plot_data("results/filter_diff_percentile.log")
