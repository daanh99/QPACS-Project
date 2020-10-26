import io
import random
import string
from dataclasses import dataclass
from typing import List, Tuple
import subprocess

from src.GoogleCloudInfo import GoogleCloudInfo, cloud_info_list
from src.MasterNode import MasterNode
from src.Node import SSHNodeError
from src.SlaveNode import SlaveNode

ex_id = ''.join(random.choice(string.ascii_lowercase) for i in range(8))  # Generate a random project id
EX_RUNTIME = 300  # seconds
print(f"Experiment ID: {ex_id}")
max_account_cpus = 64


def create_master_slaves(cpu_cores: int) -> Tuple[MasterNode, List[SlaveNode]]:
    master = MasterNode(f"master-{ex_id}-1", cloud_info_list[0], master=True)
    # slaves = []
    # i = 0
    # for cloud_info in cloud_info_list:
    #     while cloud_info.cpus_left >= cpu_cores:
    #         try:
    #             slaves.append(SlaveNode(f"slave-{cloud_info.ssh_user}-{i}", cloud_info, master_node=master))
    #             i += 1
    #         except SSHNodeError:
    #             pass
    # return master, slaves
    return master, []


def start_slave():
    import sys
    master_ip = sys.argv[1]
    cloud_index = int(sys.argv[2])
    region = sys.argv[3]
    index = int(sys.argv[4])
    cloud_info = cloud_info_list[cloud_index]
    print(f"Adding slave {index} to {cloud_info.ssh_user} in region {region} connecting to master {master_ip}")

    @dataclass
    class SlaveMaster:
        privip: str

    while True:
        try:
            slave = SlaveNode(f"slave-{cloud_info.ssh_user.replace('_', '')}-{index}", cloud_info, master_node=SlaveMaster(master_ip), location_string=region)
            break
        except SSHNodeError as e:
            print(e)
            pass
    with io.open('connections/slaves.csv', mode='a', encoding='utf-8') as file:
        file.write(f'{slave.pubip},{slave.privip},{slave.name},{cloud_info_list.index(slave.cloud_info)}\n')
    return


def start_master():
    master, slaves = create_master_slaves(cpu_cores=4)
    with io.open('connections/master.csv', mode='w', encoding='utf-8') as file:
        file.write('external-IP,internal-IP,name,cloud_index\n')
        file.write(f'{master.pubip},{master.privip},{master.name},{cloud_info_list.index(master.cloud_info)}\n')

    exit(1)
    with io.open('connections/slaves.csv', mode='w', encoding='utf-8') as file:
        file.write('external-IP,internal-IP,name,cloud_index\n')

    added = 0
    for cloud_index, cloud_info in enumerate(cloud_info_list):
        i = 0
        while cloud_info.cpus_left >= 4:
            if added % 8 == 0:
                input("Please press enter to continue...")
            try:
                subprocess.call(f'start venv\\scripts\\python.exe src\\create-all-servers.py {master.privip} {cloud_index} {cloud_info.get_next_free_location(4).name} {i}', shell=True)
                i += 1
                added += 1
            except SSHNodeError:
                pass


def main():
    import sys
    try:
        if len(sys.argv) > 1:
            start_slave()
            return

        start_master()
    # except Exception as e:
    #     print(e)
    finally:
        input("Finished!")


if __name__ == '__main__':
    main()
