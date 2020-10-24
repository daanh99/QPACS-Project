import mechanize
import numpy
import os
import queue
import random
import shutil
import socket
import string
import threading
import time
import urllib.request
from abc import ABC, abstractmethod
import pandas
import libcloud
import paramiko
from dataclasses import dataclass
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider
from paramiko.buffered_pipe import PipeTimeout

SSH_USER = 'sajvanleeuwen'
GCLOUD_ACCOUNT = '1033438718362-compute@developer.gserviceaccount.com'
GCLOUD_KEY_PATH = 'keys/sander.json'  # The path to the Service Account Key (a JSON file)
GCLOUD_PROJECT = 'quantitative-293316'  # GCloud project id
DESIGN_CSV = 'design.csv'  # The CSV with the experiment design
home_user = "am72ghiassi"

ComputeEngine = get_driver(Provider.GCE)

driver = ComputeEngine(GCLOUD_ACCOUNT, GCLOUD_KEY_PATH, project=GCLOUD_PROJECT)
location = [l for l in driver.list_locations() if l.id == '2000'][0]
network = [n for n in driver.ex_list_networks() if n.id ==
           '8459897754837846868'][0]

try:
    # To prevent problems with GCloud reusing IP addresses
    os.remove(f'/home/{SSH_USER}/.ssh/known_hosts')
except:
    pass

ex_id = ''.join(random.choice(string.ascii_lowercase)
                for i in range(8))  # Generate a random project id
EX_RUNTIME = 300  # seconds
print(f"Experiment ID: {ex_id}")



class Node(ABC):
    def __init__(self, driver, name, master=False, masterNode=None):
        self.driver = driver
        self.name = name
        if not master and masterNode == None:
            raise ValueError("Slave nodes need a master")
        self.master = masterNode
        self.disk = self.driver.create_volume(40, f"boot-{self.name}", image='tu-disk', location=location)
        self.node = self.driver.create_node(
            name, 'n1-standard-1', None, location=location, ex_boot_disk=self.disk)
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
            raise RuntimeError(f"Can't connect to node {self.name}")
        self.set_permissions()
        self.start_type()

    def __del__(self):
        self.close_ssh()

    def open_ssh(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
        self.ssh.connect(self.pubip, port=22, username=SSH_USER, key_filename='C:\\Users\sande\\.ssh\\google_cloud_rsa')
        self.connected = True

    def close_ssh(self):
        self.connected = False
        self.ssh.close()

    def run_command(self, command):
        print(f'Executing command: {command}')
        stdin, stdout, stderr = self.ssh.exec_command(command)
        stderr_output = stderr.read()
        if len(stderr_output) > 0:
            print(stdout.read())
            print(stderr_output)

    def set_permissions(self):
        self.run_command(f'sudo chmod -R 777 /home/{home_user}')
        self.run_command(f'echo "export SPARK_HOME=/home/{home_user}/bd/spark" >> .bashrc && source .bashrc')
        command = f'cp /home/am72ghiassi/bd/spark/conf/spark-env.sh.template /home/am72ghiassi/bd/spark/conf/spark-env.sh &&' + \
                  f'echo "SPARK_MASTER_HOST=\'{self.privip}\'" >> /home/{home_user}/bd/spark/conf/spark-env.sh'
        self.run_command(command)


    @abstractmethod
    def start_type(self):
        pass


@dataclass
class JobOptions:
    batch_size: int
    max_epochs: int


class MasterNode(Node):
    def start_type(self):
        command = f'/home/{home_user}/bd/spark/sbin/start-master.sh'
        self.run_command(command)

    def submit(self, options: JobOptions):

        command = f"/home/{home_user}/bd/spark/bin/spark-submit --master {self.privip} --driver-cores 1 " + \
                  f"--driver-memory 1G --total-executor-cores 1 --executor-cores 1 --executor-memory 1G "+ \
                  f"--py-files \"/home/{home_user}/bd/spark/lib/bigdl-0.8.0-python-api,/home/{home_user}/bd/codes/bi-rnn.py\" "+ \
                  f"--properties-file \"/home/{home_user}/bd/spark/conf/spark-bigdl.conf\" "+ \
                  f"--jars \"/home/{home_user}/bd/spark/lib/bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar\" "+ \
                  f"--conf \"spark.driver.extraClassPath=/home/{home_user}/bd/spark/lib/bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar\" "+ \
                  f"--conf \"spark.executer.extraClassPath=bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar /home/{home_user}/bd/codes/bi-rnn.py\" "+ \
                  f"--action train --dataPath \"/tmp/mnist\" --batchSize {options.batch_size} --endTriggerNum {options.max_epochs} "+ \
                  f"--learningRate 0.01 --learningrateDecay 0.0002"
        self.run_command(command)

    def cancel(self):
        br = mechanize.Browser()
        br.open(f"http://{self.pubip}:8080")

        def select_form(form):
            return form.attrs.get('action', None) == 'app/kill/'
        try:
            br.select_form(predicate=select_form)
        except mechanize._mechanize.FormNotFoundError:
            print("FormNotFoundError")
        except Exception as e:
            print("An error occurred during cancelling.")
            print(e)
        br.submit()


class SlaveNode(Node):
    def start_type(self):
        command = f'/home/{home_user}/bd/spark/sbin/start-slave.sh spark://{self.master.privip}:7077'
        self.run_command(command)

    def stop_type(self):
        command = f'/home/{home_user}/bd/spark/sbin/stop-slave.sh'
        self.run_command(command)


master = MasterNode(driver, f"master-{ex_id}-1", master=True)

experiments = pandas.read_csv(DESIGN_CSV)
experiments.columns.values[0] = 'Index'
experiments.set_index('Index')

if 'failure_rate' not in experiments:
    experiments['failure_rate'] = 0
if 'failure_duration' not in experiments:
    experiments['failure_duration'] = 0

# print(experiments)
num_slaves = max(experiments['num_nodes'])
print(f"Number of slaves: {num_slaves}")

os.makedirs(f"raw/{ex_id}", exist_ok=True)
shutil.copyfile(DESIGN_CSV, f"raw/{ex_id}/design.csv")

slaves = [SlaveNode(driver, f"slave-{ex_id}-{i}", masterNode=master) for i in range(1, int(num_slaves)+1)]

for idx, row in experiments.iterrows():
    print(f"Experiment {row['Index']}")
    options = JobOptions(int(row['batch_size']), 50)

    for slave in slaves:
        slave.start_type()
        # slave.start_failure_worker(
        #     row['failure_rate'], row['failure_duration'])

    filename = f"{int(row.Index)}-nodes{int(row.num_nodes)}-batch{options.batch_size}-epochs{options.max_epochs}-frate{row.failure_rate}-duration{row.failure_duration}-time{EX_RUNTIME}.log"

    command = f"/home/{home_user}/bd/spark/bin/spark-submit --master spark://{master.privip}:7077 --driver-cores 1 " + \
            f"--driver-memory 1G --total-executor-cores {int(row.num_nodes)} --executor-cores 1 --executor-memory 1024M " + \
            f"--py-files /home/{home_user}/bd/spark/lib/bigdl-0.11.0-python-api.zip,/home/{home_user}/bd/codes/bi-rnn.py " + \
            f"--properties-file /home/{home_user}/bd/spark/conf/spark-bigdl.conf " + \
            f"--jars /home/{home_user}/bd/spark/lib/bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar " + \
            f"--conf spark.driver.extraClassPath=/home/{home_user}/bd/spark/lib/bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar " + \
            f"--conf spark.executer.extraClassPath=bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar /home/{home_user}/bd/codes/bi-rnn.py " + \
            f"--action train --dataPath /tmp/mnist --batchSize {options.batch_size} --endTriggerNum {options.max_epochs} " + \
            f"--learningRate 0.01 --learningrateDecay 0.0002 > {ex_id}-{filename}"

    print(f"Executing command: {command}")
    stdin, stdout, stderr = master.ssh.exec_command(command, timeout=EX_RUNTIME)

    try:
        print(stdout.read())
        print(stderr.read())
    except PipeTimeout:
        print("PipeTimeout")
    except socket.timeout:
        print("Socket timeout")
    # master.cancel()
    sftp = master.ssh.open_sftp()
    sftp.get(f'{ex_id}-{filename}', f'raw/{ex_id}/{filename}')

    for slave in slaves:
        slave.stop_type()
