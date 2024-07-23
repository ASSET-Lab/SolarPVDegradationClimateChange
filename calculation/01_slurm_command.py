import subprocess
import os
import time

# 定义参数
model_indexes = [7] #0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
mounting_configs = ["close_mount_glass_glass"]
cpus_per_task = 16
mem = 64

run_time = "2:00:00"
# partition="standard"
script_dir = os.path.abspath('')
# year_list =  ['1950', '1951', '2037']
year_start=1950
# year_start=2001
year_end=2100
# year_end=2500
year_list = [str(year) for year in range(year_start, year_end + 1)]
# year_list = ['2020','2059','2060','2079','2080','2100']
year_list = ['2099']
year_list = ",".join(year_list)
for model_index in model_indexes:
    for mounting_config in mounting_configs:
        # sbatch_command = (f"sbatch --cpus-per-task={cpus_per_task} \
        #     --mem={mem}GB \
        #     --job-name=TPV_CF_rpv_gcm_{model_indexes}_from{year_start}_to_{year_end} \
        #     --account={account} \
        #     --time={run_time} \
        #     --partition={partition}\
        #     {script_dir}/run_gcm_yearslist.sbat\
        #      {model_index} {mounting_config} {year_list}")
        sbatch_command = (f"sbatch\
            --job-name=TPV_CF_rpv_gcm_{model_indexes}_from{year_start}_to_{year_end} \
            --time={run_time} \
            {script_dir}/01_Slurm_PVTemp_Vectorized_Calculation_CMIP6_GCMs.sbat\
             {model_index} {mounting_config} {year_list}")
        # sbatch_command = f"python {script_dir}/Slurm_years_list_pvtemp_vectorized_gcm.py {model_index} {mounting_config} {year_list}"

        print(f"Executing: {sbatch_command}")
        result = subprocess.run('module load anaconda', shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            result = subprocess.run(sbatch_command, shell=True, check=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            print(f"STDOUT: {result.stdout.decode('utf-8')}")
            print(f"STDERR: {result.stderr.decode('utf-8')}")
        except subprocess.CalledProcessError as e:
            print(f"Error running sbatch command: {e}")

        # 等待2秒
        time.sleep(0.2)