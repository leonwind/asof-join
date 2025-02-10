import matplotlib.pyplot as plt
import regex as re
import os

from benchmark_plotter import style, texify, colors
texify.latexify(5, 1.8)
style.set_custom_style()

L1_SIZE = 320 << 10 # 320 KiB
L2_SIZE = 10 << 20 # 10 MiB
NUM_CORES = 10

TOTAL_L1 = L1_SIZE * NUM_CORES
TOTAL_L2 = L2_SIZE * NUM_CORES

DATA_SIZES = [131, 182, 284, 488, 896, 1712, 3344, 
              6608, 13136, 26192, 52304, 104528, 
              208976, 417872, 835664, 1671248, 3342416]

NUM_POSITIONS = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048,
                 4096, 8192, 16384, 32768, 65536]


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

def _find_num_positions_fitting_cache(num_positions, cache_size):
    for i in range(len(DATA_SIZES) - 1):
        if DATA_SIZES[i] <= cache_size <= DATA_SIZES[i + 1]:
            print(i)
            lower_size, upper_size = DATA_SIZES[i], DATA_SIZES[i + 1]
            lower_pos, upper_pos = num_positions[i], num_positions[i + 1]

            fraction = (cache_size - lower_size) / (upper_size - lower_size)
            estimated_positions = lower_pos + fraction * (upper_pos - lower_pos)
            return estimated_positions
        
    if cache_size > DATA_SIZES[-1]:
        last_size = DATA_SIZES[-1]
        last_positions = num_positions[-1]
        
        factor = cache_size / last_size
        extrapolated_positions = last_positions * factor
        print(factor, extrapolated_positions)
        return extrapolated_positions

    return None


def _plot_distribution_group(distribution_name, strategy_exec_times, dir_name, log_scale=True):
    markers = ['*','o','x','^','s','D']
    l1_pos = _find_num_positions_fitting_cache(NUM_POSITIONS, L1_SIZE)
    l2_pos = _find_num_positions_fitting_cache(NUM_POSITIONS, L2_SIZE)
    print(l1_pos, l2_pos)

    plt.axvline(x=l1_pos, linewidth=1, ls='--', color=colors.colors["blue"])
    plt.axvline(x=l2_pos, linewidth=1, ls='--', color=colors.colors["blue"])

    plt.text(l1_pos, 0.4, "L1",
              ha="center",
              size=8,
              bbox=dict(boxstyle='round,pad=0.2', linewidth=0.4, fc="w", ec=colors.colors["blue"]))
    
    plt.text(l2_pos, 0.4, "L2",
              ha="center",
              size=8,
              bbox=dict(boxstyle='round,pad=0.2', linewidth=0.4, fc="w", ec=colors.colors["blue"]))

    for i, (strategy, exec_times) in enumerate(strategy_exec_times.items()):
        # Sort by num positions
        exec_times.sort(key = lambda x: x[0])
        plt.plot(*zip(*exec_times), marker=markers[i], label=strategy.title())

    if log_scale:
        plt.xscale("log")
        #plt.yscale("log")

    log_label_prefix = "[log]" if log_scale else ""
    plt.xlabel(f"Num positions {log_label_prefix}")
    plt.ylabel(f"Time [s] {log_label_prefix}")

    plt.title(distribution_name)
    plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))

    is_log_plot_path = "log_" if log_scale else ""
    filename = f"plots/{dir_name}/{is_log_plot_path}{distribution_name}_plot.pdf"
    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def plot_data(path):
    raw_data = _read_data(path)
    groups = _parse_data_into_groups(raw_data)
    dir_name = path.split("/")[1].split(".")[0]

    for distribution_name, distribution_group in groups.items():
        _plot_distribution_group(distribution_name,
                           distribution_group.strategy_exec_times,
                           dir_name,
                           log_scale=True)
        _plot_distribution_group(distribution_name,
                           distribution_group.strategy_exec_times,
                           dir_name,
                           log_scale=False)
    

if __name__ == "__main__":
    #plot_data("results/zipf_uniform_benchmark.txt")
    #plot_data("results/zipf_large_benchmark.txt")
    #plot_data("results/new_res.txt")
    #plot_data("results/last_results/benchmark.txt")

    #plot_data("results/2024-12-18/benchmark.txt")
    #plot_data("results/2025-01-22/benchmark.txt")

    #plot_data("results/2025-01-26/p90_uniform.log")

    plot_data("results/copying_vs_locking.log")

    #plot_data("results/2025-02-05/uniform_zipf.log")
