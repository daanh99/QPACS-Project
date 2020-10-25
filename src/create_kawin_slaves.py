import subprocess

from src.GoogleCloudInfo import cloud_info_list

cloud_info = cloud_info_list[2]

# for i in range(0, 14):
command = f'start venv\\scripts\\python.exe src\\create-all-servers.py 10.142.0.10 2 europe-west4-a 12'
print(command)
subprocess.call(command, shell=True)

command = f'start venv\\scripts\\python.exe src\\create-all-servers.py 10.142.0.10 2 europe-west4-a 13'
print(command)
subprocess.call(command, shell=True)