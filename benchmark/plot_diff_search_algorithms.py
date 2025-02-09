import matplotlib.pyplot as plt
import regex as re
import os

from benchmark_plotter import style, texify, colors
texify.latexify(7)
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


def milli_to_seconds(milli):
    return milli / 1_000.0


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
    return _parse_data_into_groups(raw)


def plot_data(interpolation_path, exponential_path, binary_path):
    interpolation_groups = _get_group(interpolation_path)
    exponential_groups = _get_group(exponential_path)
    binary_groups = _get_group(binary_path)


    dir_name = interpolation_path.split("/")[1].split(".")[0]

    _plot_search_algs(
        interpolation_groups["uniform"],
        exponential_groups["uniform"],
        binary_groups["uniform"],
        dir_name,
        log_scale=True)


if __name__ == "__main__":
    interpolation_path = "results/diff_search_algs/interpolation_results.log"
    exponential_path = "results/diff_search_algs/exponential_search_results.log"
    binary_path = "results/diff_search_algs/binary_search_results.log"
    plot_data(interpolation_path, exponential_path, binary_path)
