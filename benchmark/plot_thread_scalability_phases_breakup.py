import matplotlib.pyplot as plt
import regex as re
import os

from benchmark_plotter import style, texify, colors
texify.latexify(5, 1.8)
style.set_custom_style()


class Phases:
    def __init__(self):
        self.phases_times = {}

    def add_phase_time(self, phase: str, time: int):
        if phase not in self.phases_times:
            self.phases_times[phase] = time
        self.phases_times[phase] = min(self.phases_times[phase], time)


class StrategyRun:
    def __init__(self, strategy_name, num_threads, total_time, phases_times):
        self.strategy_name = strategy_name
        self.num_threads = num_threads
        self.total_time = total_time
        self.phases_times = phases_times


    def __repr__(self):
        return (f"StrategyRun({self.strategy_name}, num_threads={self.num_threads}, total_time={self.total_time}, "
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
            total_time = int(parts[1])
            
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


def plot_data(path):
    raw_data = _read_data(path)
    groups = _parse_data_into_groups(raw_data)
    dir_name = path.split("/")[1].split(".")[0]

    print(groups)
    print(dir_name)

    
if __name__ == "__main__":
    plot_data("results/thread_scalability/thread_scalability_phases.log")
