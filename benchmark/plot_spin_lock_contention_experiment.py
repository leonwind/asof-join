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
            continue

        if row.startswith("Total contended time:"):
            contended_time_str = row.split(": ")[1]
            total_contended_durations_ns.append(int(contended_time_str))
            continue

        if row.startswith("Left partitioning time: "):
            exec_time = int(row.split(": ")[1])
            if total_contended_durations_ns:
                min_contended_time = nano_to_micro(min(total_contended_durations_ns)) / 20.0
            else:
                min_contended_time = 0
            contended_percentage = min_contended_time / exec_time

            contended_times.append((
                curr_num_positions,
                micro_to_seconds(exec_time),
                micro_to_seconds(min_contended_time),
                contended_percentage))

    contended_times.sort(key=lambda x: x[0])
    return contended_times


def _parse_data_num_tries(data):
    contended_times = []
    group_pattern = r"\[(\d+)\.csv\]"

    curr_num_positions = None
    avg_tries = []
    for row in data:
        if row.startswith("Run"):
            match = re.search(group_pattern, row)
            curr_num_positions = int(match.group(1))
            # Reset
            avg_tries = []
            continue

        if row.startswith("Avg num tries:"):
            num_tries_str = row.split(": ")[1]
            avg_tries.append(float(num_tries_str))
            continue

        if row.startswith("Left partitioning time: "):
            exec_time = int(row.split(": ")[1])
            if avg_tries:
                min_avg_tries = min(avg_tries)
            else:
                print(f"Error on row {row}")

            contended_times.append((
                curr_num_positions,
                micro_to_seconds(exec_time),
                min_avg_tries,
                0))

    contended_times.sort(key=lambda x: x[0])
    return contended_times


def _plot_graphs(data, path):
    """ data = [(num_positions, exec_time_s, contention_time_s, contention_percentage)] """
    num_positions = [x[0] for x in data]
    exec_time = [x[1] for x in data]
    contention_time = [x[2] for x in data]
    contention_prctg = [x[3] for x in data]

    plt.plot(num_positions, exec_time, label="Spinlock exec")
    plt.plot(num_positions, contention_time, label="Spinlock contention")
    #plt.plot(num_positions, contention_prctg, label="contention percentage")

    plt.xlabel(("Num positions"))
    plt.ylabel("Time [s]")

    #plt.xscale("log")
    #plt.yscale("log")

    plt.legend()

    idx = 2 if "2" in path else 1
    filename = f"plots/contention_experiment/contention_experiment_{idx}.pdf"
    if "atomic" in path:
        filename = "plots/contention_experiment/atomic_experiment.pdf"
    
    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def _plot_spinlock_vs_atomic_exec_time(spinlock, atomic):
    """ data = [(num_positions, exec_time_s, contention_time_s, _)] """

    num_positions = [x[0] for x in spinlock]
    spinlock_exec = [x[1] for x in spinlock]
    atomic_exec = [x[1] for x in atomic]

    plt.plot(num_positions, spinlock_exec, label="Spinlock exec")
    plt.plot(num_positions, atomic_exec, label="Atomic CAS exec")

    plt.xlabel("Time [s] [log]")
    plt.ylabel("Num Retries [log]")

    plt.xscale("log")
    plt.yscale("log")

    plt.legend()

    filename = "plots/contention_experiment/cas_vs_spinlock_exec_time.pdf"
    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def _plot_spinlock_vs_atomic_retries(spinlock, atomic):
    """ data = [(num_positions, exec_time_s, contention_time_s, _)] """

    num_positions = [x[0] for x in spinlock]
    spinlock_contention_time = [x[2] for x in spinlock]
    atomic_retries = [x[2] for x in atomic]

    plt.plot(num_positions, spinlock_contention_time, label="Spinning Tries")
    plt.plot(num_positions, atomic_retries, label="CAS Tries")

    plt.xlabel("Num positions")
    plt.ylabel("Num Retries")

    plt.xscale("log")
    plt.yscale("log")

    plt.legend()

    filename = "plots/contention_experiment/cas_vs_spinlock_retries.pdf"
    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def plot_data(path):
    raw_data = _read_data(path)
    contention_data = _parse_data(raw_data)
    _plot_graphs(contention_data, path)


def plot_spin_lock_vs_atomic(spin_lock_path, atomic_path):
    spin_lock_raw = _read_data(spin_lock_path)
    atomic_raw = _read_data(atomic_path)

    spin_lock_data = _parse_data_num_tries(spin_lock_raw)
    atomic_data = _parse_data_num_tries(atomic_raw)
    print(spin_lock_data)
    print(atomic_data)

    _plot_spinlock_vs_atomic_exec_time(spin_lock_data, atomic_data)
    _plot_spinlock_vs_atomic_retries(spin_lock_data, atomic_data)



if __name__ == "__main__":
    #plot_data("results/spin_lock_contention_experiment.log")
    #plot_data("results/spin_lock_contention_experiment_2.log")
    #plot_data("results/small_benchmark_atomic_cas.log")

    #plot_spin_lock_vs_atomic(
    #    "results/spin_lock_contention_experiment_2.log",
    #    "results/small_benchmark_atomic_cas.log")

    plot_spin_lock_vs_atomic(
        "results/num_lock_tries.log",
        "results/num_cas_tries.log")