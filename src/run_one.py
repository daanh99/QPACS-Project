import subprocess

cloud_cpus_left = [16, 0, 0]
regions = [
    'us-west4-a',
    'us-east4-c',
    'us-central1-a',
    'us-east1-b',
    'us-west1-b',
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
    raise RuntimeError('Cannot find new cloud!')


def get_next_region():
    for i in range(len(cpus_left)):
        if cpus_left[i] >= 8:
            cpus_left[i] -= 8
            return regions[i]
    raise RuntimeError('Cannot find new region!')

for cpu_cores in [2, 4]:
    for amount_ram in [2048, 4096]:
        for network in ['bi-rnn', 'lenet5']:
            for batch_size in [64, 256]:
                if [cpu_cores, amount_ram, network, batch_size] in done:
                    continue
                cloud_index = get_next_cloud()
                region = get_next_region()
                command = f'start venv\\Scripts\\python.exe src\\find_runtime_one.py {cloud_index} {region} {cpu_cores} {amount_ram} {network} {batch_size}'
                print(command)
        # subprocess.call(f'start C:\\Users\\sande\\Documents\\Courses\\2020-2021\\QPEC\\QPACS-Project\\venv\\Scripts\\python.exe src\\find_runtime_one.py {cloud_index} {region} {cpu_cores} {amount_ram} lenet5 64', shell=True)
                subprocess.call(command, shell=True)