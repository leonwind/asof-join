import os
import subprocess

def _run_benchmark(prices_path, positions_path, title):
    cmd = f"../build-benchmark/asof_benchmark {prices_path} {positions_path}"
    #cmd = f"../debug/asof_benchmark {prices_path} {positions_path}"
    #print(cmd)
    
    output_err = open("output/error.txt", 'a')
    output = subprocess.run(cmd.split(" "), capture_output=True, text=True)
    print(output)


def run_benchmarks():
    #prices_path = "../data/zipf_1_5_prices_70_2000000.csv"
    prices_path = "../data/prices_small.csv"
    positions_dirs = [
        "../data/uniform",
        "../data/zipf_1_1",
        "../data/zipf_1_3",
        "../data/zipf_1_5",
        "../data/zipf_2"]
    
    output_path = "output/output.txt"
    # Clear content in output file
    open(output_path, 'w').close()
    
    output_file = open(output_path, 'a')
    for positions_dir in positions_dirs:
        for positions_file in os.listdir(positions_dir):
            positions_path = f"{positions_dir}/{positions_file}"
            description = f"Next: {positions_dir.split('/')[-1]}-{positions_file.split('.csv')[0]}"
            print(description)

            _run_benchmark(
                prices_path, 
                positions_path, 
                output_file,
                description)
    
    output_file.close()
        

if __name__ == "__main__":
    run_benchmarks()
