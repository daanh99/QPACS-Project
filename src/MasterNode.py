import mechanize

from .Node import Node


class MasterNode(Node):
    def start_type(self):
        command = f'/home/am72ghiassi/bd/spark/sbin/start-master.sh'
        self.run_command(command)

    def stop_type(self):
        command = f'/home/am72ghiassi/bd/spark/sbin/stop-master.sh'
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
            print("An error occurred during cancelloing.")
            print(e)
        br.submit()
