import subprocess
import os
import time

model_indexes = [7] #0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
mounting_configs = ["close_mount_glass_glass"]
cpus_per_task = 32
mem = 64

run_time = "1:30:00"
script_dir = os.path.abspath('')
year_start=1950
year_end=2022
years_start_list = [i for i in range(year_start, year_end + 1)]


for model_index in model_indexes:
    for mounting_config in mounting_configs:
        for year_start in years_start_list:
            
            year_list = [str(year) for year in range(year_start, year_start + 1)]
            year_list = ",".join(year_list)
            sbatch_command = (f"sbatch\
                --job-name=ERA5_TPV_CF_{year_start} \
                --time={run_time} \
                {script_dir}/03_Slurm_T98_Calculation_ERA5.sbat\
                 {model_index} {mounting_config} {year_list}")
            # sbatch_command = f"python {script_dir}/Slurm_years_list_pvtemp_vectorized_gcm.py {model_index} {mounting_config} {year_list}"
    
            print(f"Executing: {sbatch_command}")
            result = subprocess.run('module load anaconda', shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # 执行sbatch命令
            try:
                result = subprocess.run(sbatch_command, shell=True, check=True, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
                print(f"STDOUT: {result.stdout.decode('utf-8')}")
                print(f"STDERR: {result.stderr.decode('utf-8')}")
            except subprocess.CalledProcessError as e:
                print(f"Error running sbatch command: {e}")
    
            # 等待2秒
            time.sleep(0.2)
