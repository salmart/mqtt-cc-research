import numpy as np
from scipy.optimize import linear_sum_assignment
import dbutil as db
#Made by Chineme "Derek" Uba

import sqlite3



def hungarian_algorithm(cost_matrix):

    """
    Solves the assignment problem using the Hungarian algorithm.

    Args:
        cost_matrix (numpy.ndarray): A 2D square array representing the cost
            matrix, where cost_matrix[i][j] is the cost of assigning
            worker i to task j.

    Returns:
        tuple: A tuple containing two arrays:
            - row_ind: An array of row indices representing the optimal
              assignment of workers.
            - col_ind: An array of column indices representing the optimal
              assignment of tasks.
    """
    workers, task = cost_matrix.shape
    if(workers == task):
       total_assignment_arr =[]
       row_ind, col_ind = linear_sum_assignment(cost_matrix)
       print("Optimal assignment:")
       for i in range(len(row_ind)):
        print(f"Worker {row_ind[i] + 1} assigned to task {col_ind[i] + 1}")
        total_assignment_arr.append((row_ind[i], col_ind[i]))

        
       return total_assignment_arr#row_ind, col_ind #Can also return total_assignment

       
    if(workers > task):
        max_in_matrix = np.max(cost_matrix)
        col_vector = np.full((cost_matrix.shape[0], 1), max_in_matrix)
        total_assignment_arr =[]
        for i in range(workers-task):
            cost_matrix = np.hstack((cost_matrix, col_vector))
            
        #print(cost_matrix)
        row_ind, col_ind = linear_sum_assignment(cost_matrix)
        print("Optimal assignment:")
        for i in range(len(row_ind)):
            if(col_ind[i]< task):
                print(f"Worker {row_ind[i] + 1} assigned to task {col_ind[i] + 1}")
                total_assignment_arr.append((row_ind[i], col_ind[i] ))

        print("total assignment: ", total_assignment_arr)#(Worker, Task they DO)
        return total_assignment_arr#row_ind, col_ind


    if(task > workers):
        return skewed_hungarian(cost_matrix)
    
    #row_ind, col_ind = linear_sum_assignment(cost_matrix)
    #return row_ind, col_ind




#Skewed Hungarian
def skewed_hungarian(cost_matrix):#workers < tasks
    cost_matrix = cost_matrix.copy()
    n, m = cost_matrix.shape
    original_n =n

    
###################################################################
#################          EDIT BELOW      ########################
####################################################################

    if n < m:
        rem = m % n
        loop_time =  m//n

        #Divide & Conquer
        #This essentially makes it so that the last publisher isn't doing all the work
        total = rem + loop_time
        total_assignment_arr = []
        for i in range(0, total):
            matrix = cost_matrix[:,i*n:(i+1)*n]
            m,n = matrix.shape

            #Solve the broken up array
            if(m==n):
                row_ind, col_ind = hungarian_algorithm(matrix)
                
                for j in range(len(row_ind)):
                    print(f"Worker {row_ind[j] + 1} assigned to task {i*original_n +col_ind[j] + 1}") #This can be commented out
                    total_assignment_arr.append((row_ind[j], i*original_n +col_ind[j] ))

            else:
                # other stuff WORK ON THIS PART
                row_ind, col_ind = hungarian_algorithm(matrix)
                for j in range(rem):#This 
                    print(f"Worker {row_ind[j] + 1} assigned to task {i*original_n +col_ind[j] + 1}") #This can be commented out
                    total_assignment_arr.append((row_ind[j], i*original_n +col_ind[j]))
    
    return total_assignment_arr



# matrix1 = np.array([[0.4, 0.1],  #These could be the energy draw for each tasks
#                      [0.6, 0.6],
#                      [0.3, 0.2]])

#Rows would be IOT Devices/Workers/Publishers
#Columns would be Tasks
# print("===================================================================")
# print(hungarian_algorithm(matrix1))
