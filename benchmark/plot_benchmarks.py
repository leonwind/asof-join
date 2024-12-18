import matplotlib.pyplot as plt
import regex as re
import os

from benchmark_plotter import style, texify, colors
texify.latexify(5, 1.8)
style.set_custom_style()


def _read_data(path):
    with open(path, 'r') as f:
        data = f.read().splitlines()
    return data


def _parse_data(data):
    groups = {}
    group_pattern = pattern = r'\[(\w+)-(\d+)\.csv\]'

    curr_distribution = None
    curr_num_positions = None
    for row in data:
        if row.startswith("Run"):
            match = re.search(group_pattern, row)
            curr_distribution = match.group(1)
            curr_num_positions = int(match.group(2))
            if curr_distribution not in groups:
                groups[curr_distribution] = {curr_num_positions: []}
            else:
                groups[curr_distribution][curr_num_positions] = []

            continue
        
        exec_time = row.split(": ")[1]
        groups[curr_distribution][curr_num_positions].append(int(exec_time) / 1000.0)
    
    return groups


def _plot_distribution(distribution_name, num_positions, dir_name):
    data_sizes = sorted(num_positions.keys())

    left_partitioning_times = [num_positions[key][0] for key in data_sizes]
    right_partitioning_times = [num_positions[key][1] for key in data_sizes]
    both_partitioning_times = [num_positions[key][2] for key in data_sizes]
    partitioning_sort_times = [num_positions[key][3] for key in data_sizes]

    plt.plot(data_sizes, left_partitioning_times, marker="x", label='Left partitioning')
    plt.plot(data_sizes, right_partitioning_times, marker="o", label='Right partitioning')
    plt.plot(data_sizes, both_partitioning_times, marker="+", label='Both Partitioning + Sort Right')
    plt.plot(data_sizes, partitioning_sort_times, marker="v", label='Sort partitioning')

    plt.xscale("log")
    plt.yscale("log")

    plt.xlabel("Num positions")
    plt.ylabel("Time [s]")
    #plt.ticklabel_format(style="plain")

    plt.title(distribution_name)
    plt.legend()

    filename = f"plots/{dir_name}/{distribution_name}_plot.pdf"
    print(filename)
    plt.savefig(filename, dpi=400)
    os.system(f"pdfcrop {filename} {filename}")

    #plt.show()
    plt.close()


def plot_data(path):
    raw_data = _read_data(path)
    groups = _parse_data(raw_data)
    dir_name = path.split("/")[1].split(".")[0]

    for distribution, num_positions in groups.items():
        _plot_distribution(distribution, num_positions, dir_name)
    

if __name__ == "__main__":
    #plot_data("results/zipf_uniform_benchmark.txt")
    #plot_data("results/zipf_large_benchmark.txt")
    #plot_data("results/new_res.txt")
    plot_data("results/last_results/benchmark.txt")
