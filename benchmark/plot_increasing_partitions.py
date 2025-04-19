import matplotlib.pyplot as plt
import os

from benchmark_plotter import style, texify, colors
texify.latexify(fig_width=3.39, fig_height=5)
style.set_custom_style()


def micro_to_seconds(micro):
    return micro / 1_000_000.0


def _read_data(path):
    with open(path, 'r') as f:
        data = f.read().splitlines()
    return data


def parse_data(data):
    lp_inc_part_times = []
    rp_inc_part_times = []

    num_partitions = None
    for row in data:
        if row.startswith("Num Partitions:"):
            parts = row.split(": ")
            num_partitions = int(parts[1])
            continue 
            
        parts = row.split(": ")
        exec_time = micro_to_seconds(int(parts[1]))

        if "LEFT" in row:
            lp_inc_part_times.append((num_partitions, exec_time))
        else:
            rp_inc_part_times.append((num_partitions, exec_time))

    return lp_inc_part_times, rp_inc_part_times


def parse_data_inc_partitions_inc_positions(raw_data):
    data = {}

    lp_data = None
    rp_data = None
    curr_num_positions = None
    curr_num_partitions = None

    for row in raw_data:
        if row.startswith("Num Positions:"):
            if lp_data is not None:
                data[curr_num_positions] = (lp_data, rp_data)
            curr_num_positions = int(row.split("Num Positions: ")[1])
            lp_data = []
            rp_data = []
            continue

        if row.startswith("Num Partitions:"):
            curr_num_partitions = int(row.split(": ")[1])
            continue

        parts = row.split(": ")
        exec_time = micro_to_seconds(int(parts[1]))

        if "LEFT" in row:
            lp_data.append((curr_num_partitions, exec_time))
        else:
            rp_data.append((curr_num_partitions, exec_time))

    data[curr_num_positions] = (lp_data, rp_data)
    return data


def plot(lp_data, rp_data):
    lp_speedup = []
    rp_speedup = []

    lp_head_speed = lp_data[0][1]
    rp_head_speed = rp_data[0][1]
    for i in range(len(lp_data)):
        num_part = lp_data[i][0]
        lp_speedup.append((num_part, lp_head_speed / lp_data[i][1]))
        rp_speedup.append((num_part, rp_head_speed / rp_data[i][1]))

    fig, axes = plt.subplots(1, 2) 

    axes[0].plot(*zip(*lp_data), label="Left Partiton Join")
    axes[0].plot(*zip(*rp_data), label="Right Partition Join")

    axes[1].plot(*zip(*lp_speedup))
    axes[1].plot(*zip(*rp_speedup))

    axes[0].set_xscale("log")
    axes[1].set_xscale("log")

    axes[0].set_xticks([10, 1000, 100000])
    axes[1].set_xticks([10, 1000, 100000])

    axes[0].set_ylabel("Time [s]")
    axes[1].set_ylabel("Speed Up")

    fig.text(0.56, 0, f"Number of Partitions [log]", ha='center')
    fig.legend(loc="upper center", ncols=3, bbox_to_anchor=(0.56, 1.1))

    plt.tight_layout()

    filename = f"plots/skylake_final/increasing_num_partitions.pdf"
    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def plot_increasing_partitons(path):
    raw_data = _read_data(path)
    lp_data, rp_data = parse_data(raw_data)
    print(lp_data, rp_data)
    plot(lp_data, rp_data)


def plot_increasing_partitions_increasing_positions(path):
    raw_data = _read_data(path)
    parsed_data = parse_data_inc_partitions_inc_positions(raw_data)
    print(parsed_data)
    plot_all_positions(parsed_data)


def plot_all_positions(all_data):
    print(len(all_data))
    fig, axes_grid = plt.subplots(3, 2)
    axes = axes_grid.flatten()

    for i, (num_positions, data) in enumerate(all_data.items()):
        lp_data, rp_data = data
        print(lp_data)
        print(rp_data)

        axes[i].set_title(f"Num Positions: {num_positions}")
        axes[i].plot(*zip(*lp_data))
        axes[i].plot(*zip(*rp_data))

        axes[i].set_xscale("log")

    plt.tight_layout()
    filename = f"plots/skylake_final/inc_partitions_inc_positions.pdf"
    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


if __name__ == "__main__":
    #plot_increasing_partitons("results/skylake_final/increasing_partitions.log")
    plot_increasing_partitions_increasing_positions("results/skylake_final/inc_partitions_inc_positions.log")
