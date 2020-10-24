from src.Node import Node


class SlaveNode(Node):
    def start_type(self):
        command = f'/home/am72ghiassi/bd/spark/sbin/start-slave.sh spark://{self.master.privip}:7077'
        self.run_command(command)

    def stop_type(self):
        command = f'/home/am72ghiassi/bd/spark/sbin/stop-slave.sh'
        self.run_command(command)