"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
import time
import pandas as pd
import multiprocessing
from multiprocessing import Pool
from datetime import datetime



class MultiProcessingSolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, num_of_processes=None, dataset_path=None, dataset_size=None):
        self.num_of_processes = num_of_processes
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self):


        start = time.time()
        p = Pool(processes=self.num_of_processes)
        result = p.map(self.readfile, self.distribute_rows())

        final_result=pd.DataFrame()

        for i in result:
            final_result=pd.concat([final_result,i])
        hour= final_result.groupby("Hour").sum().reset_index()

        p.close()
        p.join()
        
        msg=f'Hour Number {hour.loc[hour["Count"].idxmax(), "Hour"]} was the busiest at ATL in Nov 2021, Time taken was'
        return(msg,round(time.time() - start, 2))
        
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """

    def distribute_rows(self):
        reading_info = []
        n_rows=self.dataset_size
        n_processes=self.num_of_processes
        chunk_size = n_rows // n_processes
        skip_rows = 1
        for _ in range(n_processes):
            if skip_rows + chunk_size <= n_rows:
                reading_info.append([chunk_size, skip_rows])
            else:
                reading_info.append([n_rows - skip_rows, skip_rows])
            skip_rows += chunk_size
        return reading_info
    
    def readfile(self, chunk):
        df = pd.read_csv(self.dataset_path, nrows=chunk[0], skiprows=chunk[1],header=None)
        start_date = pd.to_datetime('2021-10-31')
        end_date = pd.to_datetime('2021-12-01')
        df[0] = pd.to_datetime(df[0])
        filtered_df = df[(df[2]=='ATL') & (df[0]>start_date) & (df[0]<end_date)& (~df[4])]

        filtered_df[7] = pd.to_numeric(filtered_df[7])
        filtered_df[7]=(filtered_df[7]/100).astype(int)
        
        grouped = filtered_df[7].value_counts().reset_index() #just to make sure both airline name and count are counted as columns
        grouped.columns=["Hour","Count"]
        grouped = grouped.sort_values(by='Hour', ascending=True)
        return grouped


if __name__ == '__main__':
    solution = MultiProcessingSolution(num_of_processes=4, dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
    result, timetaken = solution.run()
    print(result, timetaken)

