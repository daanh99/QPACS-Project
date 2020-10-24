import io
import random
import string
from typing import List, Tuple

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
    slaves = []
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


def main():
    master, slaves = create_master_slaves(cpu_cores=4)
    with io.open('connections/master.csv', mode='w', encoding='utf-8') as file:
        file.write('external-IP,internal-IP,name,cloud_index\n')
        file.write(f'{master.pubip},{master.privip},{master.name},{cloud_info_list.index(master.cloud_info)}\n')

    with io.open('connections/slaves.csv', mode='w', encoding='utf-8') as file:
        file.write('external-IP,internal-IP,name,cloud_index\n')
        for slave in slaves:
            file.write(f'{slave.pubip},{slave.privip},{slave.name},{cloud_info_list.index(slave.cloud_info)}\n')


if __name__ == '__main__':
    main()
