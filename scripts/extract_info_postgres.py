#LOAD
PATH = "/disk/scratch/imputation_project_postgres"
algorithms = ["baseline", "partitioned", "shared"]
dataset = "retailer"
missingness_ratio = ["0.05", "0.1", "0.2", "0.4", "0.6", "0.8"]

data = []
meta = []

for missingness in missingness_ratio:
    for algorithm in algorithms:
        with open(PATH+"/postgres_"+missingness+"_"+dataset+"_"+algorithm+".txt") as f:
            meta += [{'missingness': missingness, 'algorithm': algorithm}]
            lines = f.readlines()
            data.append(lines)

import pandas as pd

global_algorithm = []
global_missingness = []
global_preproc = []
global_iter = []

for i in range (len(meta)):
    metadata = meta[i]
    curr_dataset = data[i]
    
    preproc_time = 0
    iter_time = 0
    
    if metadata['algorithm'] == 'baseline':
        data_preproc = curr_dataset[:2]
        data_iter = curr_dataset[3:]
        #print(data_iter)
        for line in data_preproc:
            preproc_time += (float(line.split()[-1]))
        
        for line in data_iter:
            if line.split()[-1].replace('.','',1).isdigit():
                iter_time += (float(line.split()[-1]))

        #print("line", preproc_time)
        #print("-----", metadata['algorithm'], metadata['missingness'])
        #print(data_preproc)
    elif metadata['algorithm'] == 'partitioned':
        data_preproc = curr_dataset[:3]
        data_iter = curr_dataset[4:]
        for line in data_preproc:
            preproc_time += float(line.split()[-1])
        
        for line in data_iter:
            if line.split()[-1].replace('.','',1).isdigit():
                iter_time += float(line.split()[-1])

        #print("line", preproc_time)
        #print("-----", metadata['algorithm'], metadata['missingness'])
        #print(data_preproc)
    elif metadata['algorithm'] == 'shared':#3 rows
        data_preproc = curr_dataset[:3]
        data_iter = curr_dataset[4:]
        for line in data_preproc:
            preproc_time += float(line.split()[-1])
            
        for line in data_iter:
            if line.split()[-1].replace('.','',1).isdigit():
                iter_time += float(line.split()[-1])

    print("-----", metadata['algorithm'], metadata['missingness'])
    print("preproc: ", preproc_time, 'iter: ', iter_time)
    global_algorithm += [metadata['algorithm']]
    global_missingness += [metadata['missingness']]
    global_preproc += [preproc_time]
    global_iter += [iter_time]

d = {'algorithm': global_algorithm, 'missingness': global_missingness, 'preprocessing': global_preproc, 'iteration': global_iter}
df = pd.DataFrame(data=d)
df["preprocessing"]/=1000
df["iteration"]/=1000

print(df)
