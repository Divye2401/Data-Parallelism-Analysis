"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
# import threading
import time
import pandas as pd
from tqdm import tqdm
import threading
from threading import Thread
import time
from queue import Queue
from datetime import datetime


class ThreadingSolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, num_of_threads=None, dataset_path=None, dataset_size=None):
        self.num_of_threads = num_of_threads
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self):

        start=time.time()
        threads = []
        result_queue = Queue()
        final_result=pd.DataFrame()


        chunks = self.distribute_rows()
        for i in chunks:
            thread=Thread(target=self.readfile,args=(i,result_queue))
            threads.append(thread)

        for t in threads:
            t.start()    

        for t in threads:
            t.join()
            
            final_result=pd.concat([final_result,result_queue.get()])
            

      
        
        hour= final_result.groupby("Hour").sum().reset_index() # If an airline has the most flights for the criteria, its bound to have the highest percentage also
        
        
        msg=f'Hour Number {hour.loc[hour["Count"].idxmax(), "Hour"]} was the busiest at ATL in Nov 2021, Time taken was'
        return(msg,round(time.time() - start, 2))
    

    def distribute_rows(self):
        reading_info = []
        n_rows=self.dataset_size
        n_threads=self.num_of_threads
        chunk_size = n_rows // n_threads
        skip_rows = 1
        for _ in range(n_threads):
            if skip_rows + chunk_size <= n_rows:
                reading_info.append([chunk_size, skip_rows])
            else:
                reading_info.append([n_rows - skip_rows, skip_rows])
            skip_rows += chunk_size
        return reading_info
    
    def readfile(self, chunk,result):
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
        result.put(grouped)



if __name__ == '__main__':
    solution = ThreadingSolution(num_of_threads=4, dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
    answer, timetaken = solution.run()
    print(answer, timetaken)

