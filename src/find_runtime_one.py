import os
import random
import socket
import string
from typing import List

from paramiko.buffered_pipe import PipeTimeout

from src.GoogleCloudInfo import cloud_info_list
from src.MasterNode import MasterNode
from src.SlaveNode import SlaveNode


home_user = 'am72ghiassi'


def main(args: List[str]):
    if len(args) < 6:
        print("Usage: find_runtime_one.py cloud_index region n_cpus n_mb_ram network batch_size")
        exit(1)

    cloud_info = cloud_info_list[int(args[1])]
    region = args[2]
    cpu_cores = int(args[3])
    amount_ram = int(args[4])
    network = args[5]
    batch_size = int(args[6])

    print(f"On index {int(args[1])} in regin {region} cpu_cores {cpu_cores} amount_ram {amount_ram} network {network} batch_size {batch_size}")

    ex_id = ''.join(random.choice(string.ascii_lowercase) for i in range(8))  # Generate a random project id
    master = MasterNode(f"master-{ex_id}", cloud_info, master=True, location_string=region)
    SlaveNode(f"slave-{ex_id}-0", cloud_info, master_node=master, location_string=region)
    os.makedirs(f"raw/one", exist_ok=True)
    max_epochs = 50
    filename = f"nodes1-cores{cpu_cores}-memory{amount_ram}-network{network}-batchsize{batch_size}.log"
    command = f"/home/{home_user}/bd/spark/bin/spark-submit --master spark://{master.privip}:7077 --driver-cores 1 " + \
              f"--driver-memory 1G --total-executor-cores {cpu_cores} --executor-cores {cpu_cores} --executor-memory {amount_ram}M " + \
              f"--py-files /home/{home_user}/bd/spark/lib/bigdl-0.11.0-python-api.zip,/home/{home_user}/bd/codes/{network}.py " + \
              f"--properties-file /home/{home_user}/bd/spark/conf/spark-bigdl.conf " + \
              f"--jars /home/{home_user}/bd/spark/lib/bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar " + \
              f"--conf spark.driver.extraClassPath=/home/{home_user}/bd/spark/lib/bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar " + \
              f"--conf spark.executer.extraClassPath=bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar /home/{home_user}/bd/codes/{network}.py " + \
              f"--action train --dataPath /tmp/mnist --batchSize {batch_size} --endTriggerNum {max_epochs} " + \
              f"--learningRate 0.01 --learningrateDecay 0.0002 > {ex_id}-{filename}"

    print(f"Executing command: {command}")
    stdin, stdout, stderr = master.ssh.exec_command(command)

    try:
        print(stdout.read())
        print(stderr.read())
    except PipeTimeout:
        print("PipeTimeout")
    except socket.timeout:
        print("Socket timeout")
    # master.cancel()
    sftp = master.ssh.open_sftp()
    sftp.get(f'{ex_id}-{filename}', f'raw/one/{filename}')


if __name__ == '__main__':
    try:
        import sys
        main(sys.argv)
    except Exception as e:
        print(e)
    finally:
        input("finished!")
