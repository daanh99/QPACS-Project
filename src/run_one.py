import subprocess

cloud_cpus_left = [64, 32, 64]
regions = [
    'us-central1-a',
    'us-east1-b',
    'us-east4-c',
    'us-west1-b',
    'us-west4-a',
    'europe-north1-a',
    'europe-west1-b',
    'europe-west2-c'
]
cpus_left = [8,8,8,8,8,8,8,8]

prev_region = 0


def get_next_cloud():
    global cpus_left, prev_region
    for i in range(len(cloud_cpus_left)):
        if cloud_cpus_left[i] >= 8:
            if prev_region != i:
                cpus_left = [8,8,8,8,8,8,8,8]
                prev_region = i
            cloud_cpus_left[i] -= 8
            return i


def get_next_region():
    for i in range(len(cpus_left)):
        if cpus_left[i] >= 8:
            cpus_left[i] -= 8
            return regions[i]


for cpu_cores in [2, 4]:
    for amount_ram in [2048, 4096]:
        for network in ['bi-rnn', 'lenet5']:
            for batch_size in [64, 256]:
                cloud_index = get_next_cloud()
                region = get_next_region()
        # subprocess.call(f'start C:\\Users\\sande\\Documents\\Courses\\2020-2021\\QPEC\\QPACS-Project\\venv\\Scripts\\python.exe src\\find_runtime_one.py {cloud_index} {region} {cpu_cores} {amount_ram} lenet5 64', shell=True)
                subprocess.call(f'start venv\\Scripts\\python.exe src\\find_runtime_one.py {cloud_index} {region} {cpu_cores} {amount_ram} {network} {batch_size}', shell=True)