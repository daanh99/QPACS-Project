import io

import pandas as pd
import numpy as np

dataframe = pd.read_csv('complete_results.csv', delimiter=',')
np_array = dataframe.to_numpy()

target = np_array[:, 5]
factors = np.delete(np_array, 5, 1)

averages = ''
i = 0
for factor in factors.T:
    unique_levels = np.unique(factor)
    for unique_level in unique_levels:
        indices = np.where(factor == unique_level)
        print(np.average(np.take(target, indices)))
        averages += str(np.average(np.take(target, indices))) + ','
        i += 1
print(i)

with io.open('averages.csv', mode='w', encoding='utf-8') as file:
    file.write('cores_1,cores_2,ram_1,ram_2,network_1,network_2,batch_1,batch_2,nodes_1,nodes_2,nodes_3\n')
    file.write(averages)

print(averages.replace(',', '\t'))