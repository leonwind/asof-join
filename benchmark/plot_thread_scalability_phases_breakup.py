import matplotlib.pyplot as plt
import regex as re
import math
import os

from benchmark_plotter import style, texify, colors
#texify.latexify(6, 4)
style.set_custom_style()


class Phases:
    def __init__(self):
        self.phases_times = {}

    def add_phase_time(self, phase: str, time: int):
        if phase not in self.phases_times:
            self.phases_times[phase] = time
        self.phases_times[phase] = min(self.phases_times[phase], time)


class StrategyRun:
    def __init__(self, label, num_threads, total_time, phases_times):
        self.label = label 
        self.num_threads = num_threads
        self.total_time = total_time
        self.phases_times = phases_times


    def __repr__(self):
        return (f"StrategyRun({self.label}, num_threads={self.num_threads}, total_time={self.total_time}, "
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
    current_num_threads = None
    current_strategy = None 

    for row in data:
        row = row.strip()
        if not row:
            continue

        if row.startswith("Run num threads:"):
            current_num_threads = int(row.split(": ")[1])
            groups[current_num_threads] = []
            current_strategy = None

        elif " in " in row:
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

            if current_strategy is None:
                current_strategy = Phases()
            current_strategy.add_phase_time(phase_name, time_val)

        elif row.startswith("PARTITION"):
            parts = row.split(": ")
            strategy_label = parts[0].strip()
            total_time = micro_to_seconds(int(parts[1]))
            
            if current_strategy is None:
                current_strategy = Phases()
            
            strategy_run = StrategyRun(strategy_label.title(), current_num_threads, total_time, current_strategy.phases_times)
            groups[current_num_threads].append(strategy_run)
            current_strategy = None

        else:
            continue

    return groups


def _plot_strategies_time_phases(strategy_run):
    pass


def _plot_all_phases_of_competitor_separately(groups, competitor_label, dir_name):
    num_threads = []
    phases_times = {}

    for num_thread, strategy_runs in groups.items():
        total_time = 0
        for strategy_run in strategy_runs:
            if strategy_run.label != competitor_label:
                continue

            phases = strategy_run.phases_times 

            for phase_label, time in phases.items():
                if phase_label not in phases_times:
                    phases_times[phase_label] = []
                phases_times[phase_label].append(time)
                total_time += time

            total_time_label = "total execution"
            if total_time_label not in phases_times:
                phases_times[total_time_label] = []
            #phases_times[total_time_label].append(strategy_run.total_time)
            phases_times[total_time_label].append(total_time)


        num_threads.append(num_thread)

    print(num_threads)
    print(phases_times)

    num_phases = len(phases_times.keys())
    subplots_square_length = math.ceil(math.sqrt(num_phases))
    print(subplots_square_length)

    fig, axs = plt.subplots(num_phases, 1)
    plot_idx = 0

    x_ticks = num_threads[1::2]
    y_ticks = [0, 10, 20]

    for label, times in phases_times.items():
        single_thread_time = times[0]
        perfect_scale = [i for i in num_threads]
        axs[plot_idx].plot(num_threads, [times[0] / x for x in times], label=competitor_label)
        axs[plot_idx].plot(num_threads, perfect_scale, "--", label="Theoretical")

        if plot_idx == 0:
            axs[plot_idx].legend(loc="upper right")

        axs[plot_idx].set_title(label.title(), y=0.65)

        axs[plot_idx].set_xticks(x_ticks)
        axs[plot_idx].set_ylabel("Speedup")
        axs[plot_idx].set_yticks(y_ticks)

        if plot_idx == num_phases - 1:
            axs[plot_idx].set_xlabel("Number of Threads")
        else:
            axs[plot_idx].set_xticklabels([])
            axs[plot_idx].xaxis.set_ticks_position("none")
        
        plot_idx += 1
    
    competitor_name_file = competitor_label.replace(" ", "_").lower()
    filename = f"plots/{dir_name}/{competitor_name_file}_phases_breakup_plot.pdf"
    print(f"Plotting {filename}")

    plt.savefig(filename, dpi=400)
    os.system(f"pdfcrop {filename} {filename}")

    plt.close()


def plot_data(path):
    raw_data = _read_data(path)
    groups = _parse_data_into_groups(raw_data)
    dir_name = path.split("/")[1].split(".")[0]

    #print(groups)
    #print(dir_name)

    _plot_all_phases_of_competitor_separately(groups, "Partition Right", dir_name)
    _plot_all_phases_of_competitor_separately(groups, "Partition Left", dir_name)

    
if __name__ == "__main__":
    #plot_data("results/thread_scalability/thread_scalability_phases.log")
    plot_data("results/skylake/thread_scalability.log")
