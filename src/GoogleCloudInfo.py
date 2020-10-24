import io
import json
from typing import Optional

from libcloud.compute.base import NodeLocation
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider


ComputeEngine = get_driver(Provider.GCE)


def _get_region(location: NodeLocation) -> str:
    return location.name[:location.name.rindex('-')]


class GoogleCloudInfo:
    def __init__(self, ssh_user: str, disk_name: str, gcloud_key_path: str, ssh_key_path: str, max_cpus: int):
        with io.open(gcloud_key_path, mode='r', encoding='utf-8') as file:
            data = json.load(file)
        self.ssh_user = ssh_user
        self.gcloud_account = data['client_email']
        self.gcloud_project = data['project_id']
        self.gcloud_key_path = gcloud_key_path
        self.ssh_key_path = ssh_key_path
        self.cpus_left = max_cpus
        self.disk_name = disk_name

        self.driver = ComputeEngine(self.gcloud_account, self.gcloud_key_path, project=self.gcloud_project)
        self.locations = self.driver.list_locations()
        self.used_locations = {}

    def get_next_free_location(self, new_cpu_cores: int) -> Optional[NodeLocation]:
        if self.cpus_left - new_cpu_cores < 0:
            return None
        for location in self.locations:
            region = _get_region(location)
            if region not in self.used_locations:
                self.used_locations[region] = 8
            if self.used_locations[region] - new_cpu_cores >= 0:
                self.used_locations[region] -= new_cpu_cores
                self.cpus_left -= new_cpu_cores
                return location
        return None


cloud_info_list = [
    GoogleCloudInfo('sajvanleeuwen', 'tu-disk', 'keys/sander.json', 'keys/sander_ssh_key', 100),
    GoogleCloudInfo('daanhofman1', 'image-cs4215', 'keys/daan.json', 'keys/daan_ssh_key', 32),
    GoogleCloudInfo('kawin_zheng', 'image-1', 'keys/kawin.json', 'keys/kawin_ssh_key', 100)
]
