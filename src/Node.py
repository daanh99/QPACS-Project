import time
from abc import ABC, abstractmethod

import paramiko

from src.GoogleCloudInfo import GoogleCloudInfo


class SSHNodeError(RuntimeError):
    pass

class Node(ABC):
    def __init__(self, name, cloud_info: GoogleCloudInfo, master=False, master_node=None, location_string=None):
        self.driver = cloud_info.driver
        self.name = name
        self.cloud_info = cloud_info
        if not master and master_node == None:
            raise ValueError("Slave nodes need a master")
        self.master = master_node
        location = None
        if location_string is None:
            location = self.cloud_info.get_next_free_location(4)
        else:
            for loc in self.cloud_info.locations:
                if loc.name == location_string:
                    location = loc
                    break
            if location is None:
                raise RuntimeError("Your region was not found!")
        self.disk = self.driver.create_volume(40, f"boot-{self.name}", image=self.cloud_info.disk_name, location=location)
        self.node = self.driver.create_node(name, 'n2-standard-4', None, location=location, ex_boot_disk=self.disk)
        self.driver.wait_until_running([self.node])
        self.pubip = self.node.public_ips[0]
        self.privip = self.node.private_ips[0]
        self.connected = False

        for i in range(5):  # Try 5 times
            try:
                self.open_ssh()
                break
            except Exception as e:
                print(e)
                time.sleep(5)
        if not self.connected:
            raise SSHNodeError(f"Can't connect to node {self.name}")
        self.set_permissions()
        self.start_type()

    def __del__(self):
        self.close_ssh()

    def open_ssh(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
        self.ssh.connect(self.pubip, port=22, username=self.cloud_info.ssh_user, key_filename=self.cloud_info.ssh_key_path)
        self.connected = True

    def close_ssh(self):
        self.connected = False
        self.ssh.close()

    def destroy(self):
        self.driver.destroy_node(self.node)

    def run_command(self, command: str):
        print(f"Executing command: {command}")
        stdin, stdout, stderr = self.ssh.exec_command(command)
        stderr_output = stderr.read()
        if len(stderr_output) > 0:
            print(stdout.read())
            print(stderr_output)

    def set_permissions(self):
        self.run_command(f'sudo chmod -R 777 /home/am72ghiassi')
        self.run_command(f'echo "export SPARK_HOME=/home/am72ghiassi/bd/spark" >> .bashrc && source .bashrc')
        command = f'cp /home/am72ghiassi/bd/spark/conf/spark-env.sh.template /home/am72ghiassi/bd/spark/conf/spark-env.sh &&' + \
                  f'echo "SPARK_MASTER_HOST=\'{self.privip}\'" >> /home/am72ghiassi/bd/spark/conf/spark-env.sh'
        self.run_command(command)

    @abstractmethod
    def start_type(self):
        pass