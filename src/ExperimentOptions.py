class ExperimentOptions:
    def __init__(self, cpu_cores: int, amount_ram: int, network: str, batch_size: int, amount_nodes: int):
        self.cpu_cores = cpu_cores
        self.amount_ram = amount_ram
        self.network = network
        self.batch_size = batch_size
        self.amount_nodes = amount_nodes

    def get_array(self):
        return [self.cpu_cores, self.amount_ram, self.network, self.batch_size, self.amount_nodes]

    def get_filename(self):
        return f"nodes{self.amount_nodes}-cores{self.cpu_cores}-memory{self.amount_ram}-network{self.network}-batchsize{self.batch_size}.log"

    def get_csv_row(self):
        return f"{self.cpu_cores},{self.amount_ram},{self.network},{self.batch_size},{self.amount_nodes}"
