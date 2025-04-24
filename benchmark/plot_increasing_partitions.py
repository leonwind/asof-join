import matplotlib.pyplot as plt
import os
import matplotlib.transforms as mtransforms


from benchmark_plotter import style, texify, colors
texify.latexify(fig_width=3.39, fig_height=4)
style.set_custom_style()


LP_SIZES = [
    99000272, # 1M
    247500288,
    495000272, # 5M
    990000272, # 10M
    4950000272, # 50M
    9900000272 # 100M
]

RP_SIZE = 7500000272

L1_SIZE = 384 << 10 # 384 KiB
L2_SIZE = 12 << 20 # 12 MiB
L3_SIZE = 19.3 * 2**20 # 19.3 MIB

NUM_CORES = 12
TOTAL_L1 = L1_SIZE * NUM_CORES
TOTAL_L2 = L2_SIZE * NUM_CORES
TOTAL_CACHE = TOTAL_L1 + TOTAL_L2 + L3_SIZE


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


def _find_num_partitions_fitting_caches(data_size, cache_size):
    return data_size / cache_size


def plot_all_positions(all_data):
    print(len(all_data))
    fig, axes_grid = plt.subplots(3, 2, sharex=True)
    axes = axes_grid.flatten()

    rp_l1_pos = _find_num_partitions_fitting_caches(RP_SIZE, L1_SIZE)
    rp_l2_pos = _find_num_partitions_fitting_caches(RP_SIZE, L2_SIZE)
    rp_l3_pos = _find_num_partitions_fitting_caches(RP_SIZE, L2_SIZE + L3_SIZE)

    rel_cache_text_pos = 0.18


    for i, (num_positions, data) in enumerate(sorted(all_data.items())):
        lp_data, rp_data = data
        print(lp_data)
        print(rp_data)

        print(f"LP Fitting Partitions in L1 for {num_positions} Tuples: \
              {_find_num_partitions_fitting_caches(LP_SIZES[i], L1_SIZE)}")
        print(f"LP Fitting Partitions in L2 for {num_positions} Tuples: \
              {_find_num_partitions_fitting_caches(LP_SIZES[i], L2_SIZE)}")

        lp_l1_pos = _find_num_partitions_fitting_caches(LP_SIZES[i], L1_SIZE)
        lp_l2_pos = _find_num_partitions_fitting_caches(LP_SIZES[i], L2_SIZE)

        axes[i].axvline(x=lp_l1_pos, linewidth=1, ls='--', color=colors.colors["blue"])
        #axes[i].axvline(x=lp_l2_pos, linewidth=1, ls='--', color=colors.colors["blue"])

        axes[i].axvline(x=rp_l1_pos, linewidth=1, ls='--', color=colors.colors["orange"])
        #axes[i].axvline(x=rp_l3_pos, linewidth=1, ls='--', color=colors.colors["orange"])

        axes[i].text(lp_l1_pos, rel_cache_text_pos, "L1", ha="center", size=8,
                transform=mtransforms.blended_transform_factory(axes[i].transData, axes[i].transAxes),
                bbox=dict(boxstyle='round,pad=0.2', linewidth=0.4, fc="w", ec=colors.colors["blue"]))

        axes[i].text(rp_l1_pos, rel_cache_text_pos, "L1", ha="center", size=8,
                transform=mtransforms.blended_transform_factory(axes[i].transData, axes[i].transAxes),
                bbox=dict(boxstyle='round,pad=0.2', linewidth=0.4, fc="w", ec=colors.colors["orange"]))

        axes[i].set_title(f"Left Rel. Size: {(num_positions/1000000):g}M")
        axes[i].plot(*zip(*lp_data), label="Left Partition Join")
        axes[i].plot(*zip(*rp_data), label="Right Partition Join")

        axes[i].set_xscale("log")

        axes[i].set_xticks([10, 1000, 100000])
        axes[i].set_xticklabels(["$10^1$", "$10^3$", "$10^5$"])

        axes[i].set_ylim(ymin=0)

        if i == 0:
            fig.legend(loc="upper center", ncols=2, bbox_to_anchor=(0.58, 1.04), frameon=False)

        # Middle on the left column
        if i == 2:
            axes[i].set_ylabel("Time [s]")

        # Bottom Row
        if i < 4:
            axes[i].xaxis.set_ticks_position("none")

    fig.text(0.56, 0, f"Number of Partitions [log]", ha='center')


    plt.tight_layout()
    filename = f"plots/skylake_final/inc_partitions_inc_positions.pdf"
    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


if __name__ == "__main__":
    #plot_increasing_partitons("results/skylake_final/increasing_partitions.log")
    plot_increasing_partitions_increasing_positions("results/skylake_final/inc_partitions_inc_positions_new.log")
