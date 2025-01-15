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


def nano_to_seconds(nano):
    return nano / 1_000_000_000.0

def nano_to_micro(nano):
    return nano / 1_000.0

def micro_to_seconds(micro):
    return micro / 1_000_000.0

def milli_to_seconds(milli):
    return milli / 1_000.0


def _parse_data(data):
    contended_times = []
    group_pattern = r"\[(\d+)\.csv\]"

    curr_num_positions = None
    total_contended_durations_ns = []
    for row in data:
        if row.startswith("Run"):
            match = re.search(group_pattern, row)
            curr_num_positions = int(match.group(1))
            # Reset contention duration
            total_contended_durations_ns = []
            print("Curr num positions:", curr_num_positions)
            continue

        if row.startswith("Total contended time:"):
            contended_time_str = row.split(": ")[1]
            total_contended_durations_ns.append(int(contended_time_str))
            #print("Printing contention duration:", total_contended_durations_ns)
            continue

        if row.startswith("Left partitioning time: "):
            exec_time = int(row.split(": ")[1])
            min_contended_time = nano_to_micro(min(total_contended_durations_ns)) / 20.0

            print(micro_to_seconds(exec_time), micro_to_seconds(min_contended_time))

            contended_percentage = min_contended_time / exec_time
            contended_times.append((
                curr_num_positions,
                micro_to_seconds(exec_time),
                micro_to_seconds(min_contended_time),
                contended_percentage))

    contended_times.sort(key=lambda x: x[0])
    return contended_times


def _plot_graphs(data, path):
    """ data = [(num_positions, exec_time_s, contention_time_s, contention_percentage)]"""
    num_positions = [x[0] for x in data]
    print(num_positions)
    exec_time = [x[1] for x in data]
    contention_time = [x[2] for x in data]
    contention_prctg = [x[3] for x in data]

    plt.plot(num_positions, exec_time, label="exec time")
    plt.plot(num_positions, contention_time, label="contention time")
    plt.plot(num_positions, contention_prctg, label="contention percentage")

    #plt.xscale("log")
    #plt.yscale("log")

    plt.legend()

    idx = 2 if "2" in path else 1

    filename = f"plots/contention_experiment/contention_experiment_{idx}.pdf"
    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def plot_data(path):
    raw_data = _read_data(path)
    contended_percentage_data = _parse_data(raw_data)
    print(contended_percentage_data)

    _plot_graphs(contended_percentage_data, path)


if __name__ == "__main__":
    plot_data("results/spin_lock_contention_experiment.log")
    plot_data("results/spin_lock_contention_experiment_2.log")
