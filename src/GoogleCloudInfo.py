import io
import json
from typing import Optional

from libcloud.compute.base import NodeLocation
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider


ComputeEngine = get_driver(Provider.GCE)


class GoogleCloudInfo:
    def __init__(self, ssh_user: str, disk_name: str, gcloud_key_path: str, ssh_key_path: str):
        with io.open(gcloud_key_path, mode='r', encoding='utf-8') as file:
            data = json.load(file)
        self.ssh_user = ssh_user
        self.gcloud_account = data['client_email']
        self.gcloud_project = data['project_id']
        self.gcloud_key_path = gcloud_key_path
        self.ssh_key_path = ssh_key_path
        self.cpus_left = 64
        self.disk_name = disk_name

        self.driver = ComputeEngine(self.gcloud_account, self.gcloud_key_path, project=self.gcloud_project)
        self.locations = self.driver.list_locations()
        self.used_locations = {}

    def get_next_free_location(self, new_cpu_cores: int) -> Optional[NodeLocation]:
        if self.cpus_left - new_cpu_cores < 0:
            return None
        for location in self.locations:
            if location not in self.used_locations:
                self.used_locations[location] = 8
            if self.used_locations[location] - new_cpu_cores >= 0:
                self.used_locations[location] -= new_cpu_cores
                self.cpus_left -= new_cpu_cores
                return location
        return None


cloud_info_list = [
    GoogleCloudInfo('sajvanleeuwen', 'tu-disk', 'keys/sander.json', 'keys/sander_ssh_key'),
    GoogleCloudInfo('daanhofman1', 'image-cs4215', 'keys/daan.json', 'keys/daan_ssh_key'),
    GoogleCloudInfo('kawin_zheng', 'image-1', 'keys/kawin.json', 'keys/kawin_ssh_key')
]
