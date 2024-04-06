import pandas as pd
from mpi4py import MPI
import time
"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""

class MPISolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, dataset_path=None, dataset_size=None):
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size
        self.comm = MPI.COMM_WORLD
        self.size = self.comm.Get_size()
        self.rank = self.comm.Get_rank()

    def run(self):

        start=time.time()

        if self.rank == 0:
            chunks=self.distribute_rows()

            for worker in range(1, self.size):
                index = worker-1
                self.comm.send(chunks[index], dest=worker)

            
            final_result = pd.DataFrame()
            for worker in (range(1, self.size)): 
                    result = self.comm.recv(source=worker)
                    final_result=pd.concat([final_result,result])
                    
            airline= final_result.groupby("Airline").sum().reset_index()     
            msg=f'Airline {airline.loc[airline["Count"].idxmax(), "Airline"]} had greatest percentage of departures from codes P and S, Time taken was'
            return(msg,round(time.time() - start, 2))


        
        elif self.rank > 0:
            chunk = self.comm.recv()
            result=self.readfile(chunk)
            self.comm.send(result, dest=0)
            return None,None

        
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """

       

    def distribute_rows(self):
        reading_info = []
        n_rows=self.dataset_size
        n_processes=self.size-1
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
        filtered_df = df[(df[2].str.startswith('P') | df[2].str.startswith('S'))&~df[4]] 
        grouped = filtered_df[1].value_counts().reset_index() #just to make sure both airline name and count are counted as columns
        grouped.columns=["Airline","Count"]
        return grouped
    

if __name__ == '__main__':
    solution = MPISolution(dataset_path="C:\\Users\\divye\\Desktop\\implementation\\Combined_Flights_2021.csv", dataset_size=6311871)
    answer,timetaken=solution.run()
    print(answer, timetaken)

