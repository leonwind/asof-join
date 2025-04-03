import matplotlib.pyplot as plt
import regex as re
import math
import os

from benchmark_plotter import style, texify, colors
texify.latexify(3.39, 3)
style.set_custom_style()


class Phases:
    def __init__(self):
        self.phases_times = {}

    def add_phase_time(self, phase: str, time: int):
        if phase not in self.phases_times:
            self.phases_times[phase] = time
        self.phases_times[phase] = min(self.phases_times[phase], time)


class StrategyRun:
    def __init__(self, label, size, total_time, phases_times):
        self.label = label 
        self.size = size 
        self.total_time = total_time
        self.phases_times = phases_times


    def __repr__(self):
        return (f"StrategyRun({self.label}, positions={self.size}, total_time={self.total_time}, "
                f"phases_times={self.phases_times})")


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
    
    run_pattern = r'\[(\w+)-(\d+)\.csv\]'
    run_pattern_no_csv = r'\[(\w+)-(\d+)\]'
    
    # Variables to keep track of the current group details
    curr_distribution = None
    curr_num_positions = None
    curr_strategy = None

    for row in data:
        if not row:
            continue

        if row.startswith("Run"):
            #regex_match = re.search(run_pattern, row)
            regex_match = re.search(run_pattern_no_csv, row)
            if regex_match is None:
                print(f"Regex error: {row}")
            curr_distribution = regex_match.group(1)
            curr_num_positions = int(regex_match.group(2))
            curr_strategy = None 

            if curr_distribution not in groups:
                groups[curr_distribution] = {}
            groups[curr_distribution][curr_num_positions] = []
            continue

        if " in " in row:
            parts = row.split(" in ")
            phase_name = parts[0].strip()
            time_str = parts[1].strip() 

            if time_str.endswith("[ms]"):
                time_val = milli_to_seconds(int(time_str[:-4]))
            elif time_str.endswith("[us]"):
                time_val = micro_to_seconds(int(time_str[:-4]))
            else:
                print(f"Missing unit??: {time_str}")
                time_val = int(time_str)

            if curr_strategy is None:
                curr_strategy = Phases()
            curr_strategy.add_phase_time(phase_name, time_val)
            continue

        if row.startswith("PARTITION"):
            parts = row.split(": ")
            strategy_label = parts[0].strip()
            total_time = micro_to_seconds(int(parts[1]))
            print(row, total_time)

            strategy_run = StrategyRun(strategy_label.title(), curr_num_positions, total_time, curr_strategy.phases_times)
            groups[curr_distribution][curr_num_positions].append(strategy_run)
            
            curr_strategy = None 
    
    return groups


def get_phases_times_of_competitor(groups, competitor_label):
    phases_times = {}

    for position_size, strategy_runs in groups.items():
        total_time = 0
        for strategy_run in strategy_runs:
            if strategy_run.label != competitor_label:
                continue
            
            print(strategy_run)

            phases = strategy_run.phases_times 

            for phase_label, time in phases.items():
                if phase_label not in phases_times:
                    phases_times[phase_label] = []
                phases_times[phase_label].append((position_size, time))
                total_time += time

            total_time_label = "total execution"
            if total_time_label not in phases_times:
                phases_times[total_time_label] = []
            #phases_times[total_time_label].append(strategy_run.total_time)
            phases_times[total_time_label].append((position_size, strategy_run.total_time))

    return list(phases_times.items())


def _plot_all_phases_of_competitor_separately(groups, competitor_label, dir_name):
    phases_times = {}

    for position_size, strategy_runs in groups.items():
        total_time = 0
        for strategy_run in strategy_runs:
            if strategy_run.label != competitor_label:
                continue
            
            print(strategy_run)

            phases = strategy_run.phases_times 

            for phase_label, time in phases.items():
                if phase_label not in phases_times:
                    phases_times[phase_label] = []
                phases_times[phase_label].append((position_size, time))
                total_time += time

            total_time_label = "total execution"
            if total_time_label not in phases_times:
                phases_times[total_time_label] = []
            #phases_times[total_time_label].append(strategy_run.total_time)
            phases_times[total_time_label].append((position_size, strategy_run.total_time))

    print(phases_times)

    num_phases = len(phases_times.keys())

    cols = 2
    rows = (num_phases + 1) // 2

    fig, axs = plt.subplots(num_phases, 1)
    #fig, axs = plt.subplots(rows, cols)
    #axs = axs.flatten()

    plot_idx = 0

    for label, times in phases_times.items():
        times.sort(key = lambda x: x[0])
        num_positions, exec_times = zip(*times)
        print(exec_times)
        axs[plot_idx].plot(num_positions, exec_times, label=competitor_label)

        #if plot_idx == 0:
        #    axs[plot_idx].legend(loc="upper right")

        #axs[plot_idx].set_title(label.title(), y=0.65)

        axs[plot_idx].set_xticks(num_positions)
        axs[plot_idx].set_xscale("log")
        
        #axs[plot_idx].set_yscale("log")
        #axs[plot_idx].set_ylabel("Time [s]")

        # Left Column
        #if plot_idx % 2 == 0:
        if plot_idx == num_phases // 2:
            axs[plot_idx].set_ylabel("Time [s]")
        #axs[plot_idx].set_yticks(y_ticks)
        #axs[plot_idx].set_yticklabels(y_ticks)
        
        if plot_idx >= num_phases - 1:
            axs[plot_idx].set_xlabel("Num positions [log]")
        else:
            axs[plot_idx].set_xticklabels([])
            axs[plot_idx].xaxis.set_ticks_position("none")
        
        plot_idx += 1
    
    #if num_phases % 2 != 0:
    #    axs[-1].axis("off")
    #plt.xscale("log")

    competitor_name_file = competitor_label.replace(" ", "_").lower()
    filename = f"plots/{dir_name}/{competitor_name_file}_total_phase_breakdown_plot.pdf"
    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def plot_two_competitors_together(groups, comp_a_label, comp_b_label, dir_name):
    phases_times_a = get_phases_times_of_competitor(groups, comp_a_label)
    phases_times_b = get_phases_times_of_competitor(groups, comp_b_label)
    num_phases = max(len(phases_times_a), len(phases_times_b))

    print(phases_times_a)
    print(phases_times_b)

    if len(phases_times_a) < len(phases_times_b):
        phases_times_a, phases_times_b = phases_times_b, phases_times_a
    num_phases = len(phases_times_a)

    markers = ['*', 'o', 'x', '^', 's', 'D']
    fig, axs = plt.subplots(num_phases, 1, sharex=True)

    j = 0
    for i in range(num_phases):
        label, times_a = phases_times_a[i]
        times_a.sort(key = lambda x: x[0])
        num_positions, times_a = zip(*times_a)

        label_b, times_b = phases_times_b[j]
        if label == label_b:
            j += 1
            _, times_b = zip(*times_b)
        else:
            times_b = None

        print(num_positions, times_a)

        axs[i].plot(num_positions, times_a, label=comp_a_label)
        if times_b is not None:
            axs[i].plot(num_positions, times_b, label=comp_b_label)
        
        axs[i].set_xscale("log")
        axs[i].set_ylabel("Time [s]")

        if i == 0:
            fig.legend(loc="upper center", ncols=2, bbox_to_anchor=(0.515, 1.04))
        
        if i == num_phases - 1:
            axs[i].set_xlabel("Num Positions [log]")
        else:
            axs[i].xaxis.set_ticks_position("none")

        axs[i].set_title(label.title(), y=0.83)

    fig.align_ylabels()
    plt.subplots_adjust(hspace=0.5)

    filename = f"plots/{dir_name}/lp_vs_rp_total_phase_breakdown_plot.pdf"
    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400, bbox_inches="tight")
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def plot_data(path):
    raw_data = _read_data(path)
    groups = _parse_data_into_groups(raw_data)
    #print(groups)

    dir_name = path.split("/")[1].split(".")[0]

    print("Uniform data:")
    print(groups["uniform"])

    #_plot_all_phases_of_competitor_separately(groups["uniform"], "Partition Left", dir_name)
    #_plot_all_phases_of_competitor_separately(groups["uniform"], "Partition Right", dir_name)

    plot_two_competitors_together(groups["uniform"], "Partition Left", "Partition Right", dir_name)

 
if __name__ == "__main__":
    #plot_data("results/skylake/diff_zipf_skews_phase_breakdown_new.log")
    plot_data("results/skylake/uniform_both_sides_phases_breakdown.log")
