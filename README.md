# Big Data Analytics with Parallelism Techniques

In this programming assignment, you will perform several big data analytics tasks using different parallelism techniques, including multi-threading, multi-processing, and MPI (Message Passing Interface). The goal is to divide the problem into smaller chunks and process them concurrently and in parallel.

## Overview

The assignment involves working with a large CSV file (~2.21 GB) containing information about air flights, including airlines, flight dates, distances, origin and destination states, etc.

### Data

The dataset consists of 61 columns and provides comprehensive information about air flights. [Download Dataset](flight_data.csv)

### Implementation

The scripts have been written to answer specific metrics such as

1. **Q1**: Airline with the highest percentage of flights departing from airports with origin codes starting with 'P' or 'S' in 2021.
2. **Q2**: Determining the airline with the highest percentage of on-time arrivals in 2021.
3. **Q3**: Finding the airline with the highest percentage of flights arriving early (negative arrival delay) in the first quarter of 2021.
4. **Q4**: Identifing the busiest hour of the day for flights departing from "Hartsfield-Jackson Atlanta International Airport" (ATL) in terms of the number of departures in November 2021.

All of these problems were implemented using three parallelism techniques:

- **T1**: Multi-threading
- **T2**: Multi-processing
- **T3**: MPI (Message Passing Interface)

### Analysis

* Calculating Execution Time of MPI with increasing worker sizes
![image](https://github.com/Divye2401/Data-Parallelism-Analysis/assets/52701687/8f72ac4d-c1a7-426c-a39e-c247a1799564)

* Comparing Execution Times between the 3 different techniques
![image](https://github.com/Divye2401/Data-Parallelism-Analysis/assets/52701687/ad43fc7c-e6ab-4f39-a62b-5bbe9b70c110)



## Resources

- [Dataset](flight_data.csv)
- [MPI Documentation](https://www.open-mpi.org/)
- [Python Threading Documentation](https://docs.python.org/3/library/threading.html)
- [Python Multiprocessing Documentation](https://docs.python.org/3/library/multiprocessing.html)

## Note

Ensure proper error handling, efficient memory management, and thorough documentation of your code. Additionally, adhere to good coding practices and provide clear explanations in your report.

## Additional Information

For any queries or clarifications, please contact [insert contact details].
