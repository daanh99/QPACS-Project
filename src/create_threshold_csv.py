import io

from src.ExperimentOptions import ExperimentOptions


def read_epoch_throughput(filename: str) -> float:
    str_to_search = "Throughput is "
    # print(f"Reading {filename}")
    with io.open(filename, mode='r', encoding='utf-8') as file:
        lines = [line for line in file.read().splitlines() if '[Epoch 1 ' in line]

    if len(lines) == 0:
        raise ValueError('No lines found')

    count_throughputs = 0
    sum_throughput = 0
    for line in lines:
        try:
            start_of_throughput_index = line.index(str_to_search) + len(str_to_search)
            sum_throughput += float(line[start_of_throughput_index:line.index(' ', start_of_throughput_index)])
            count_throughputs += 1
        except ValueError:
            pass

    return sum_throughput / count_throughputs


def write_throughput(options: ExperimentOptions):
    file_prepends = ['raw\\binsearch']
    if options.amount_nodes == 1:
        file_prepends = ['raw\\one', 'raw\\one_first', 'raw\\one_second']
    total_throughput = 0
    try:
        for file_prepend in file_prepends:
            total_throughput += read_epoch_throughput(file_prepend + '\\' + options.get_filename())
    except (FileNotFoundError, ValueError):
        print(options.get_array())
        return

    throughput = total_throughput / len(file_prepends)
    with io.open('results.csv', mode='a', encoding='utf-8') as file:
        file.write(options.get_csv_row() + f',{throughput}\n')


with io.open('results.csv', mode='w', encoding='utf-8') as file:
    file.write('cores,ram,network,batch_size,nodes,throughput\n')

for cpu_cores in [2, 4]:
    for amount_ram in [2048, 4096]:
        for network in ['bi-rnn', 'lenet5']:
            for batch_size in [64, 256]:
                for amount_nodes in [1, 16, 32]:
                    options = ExperimentOptions(cpu_cores, amount_ram, network, batch_size, amount_nodes)
                    write_throughput(options)
