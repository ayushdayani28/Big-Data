##########################################################################
## streamingCSE545sp22_lastname_id.py  v1
## 
## Template code for assignment 1 part 1. 
## Do not edit anywhere except blocks where a #[TODO]# appears
##
## Student Name: Ayush Dayani
## Student ID: 114241407



from operator import mod
import sys
from pprint import pprint
from random import randint, random
from collections import deque
from sys import getsizeof
import resource
import numpy as np

##########################################################################
##########################################################################
# Methods: implement the methods of the assignment below.  
#
# Each method gets 1 100 element array for holding ints of floats. 
# This array is called memory1a, memory1b, or memory1c
# You may not store anything else outside the scope of the method.
# "current memory size" printed by main should not exceed 8,000.

MEMORY_SIZE = 100 #do not edit
memory1a =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit
def task1ADistinctValues(element, returnResult = True):
    #[TODO]#

    ## number of element seen so far
    if memory1a[6] == None:
        memory1a[6] = 0
    else:
        memory1a[6] += 1
    f = 10
    #process the element you may only use memory1a, storing at most 100 
    def h(x,n,a,b,c):
        y = (a*x+b)//(c)
        if y==0:
            pass
        else:
            y = format(y, 'b')
            k = 0

            ## trailing zeroes
            for item in y[::-1]:
                if item == '0':
                    k+=1
                else:
                    break
            if memory1a[-n]==None or 2**k>memory1a[-n]:
                memory1a[-n] = 2**k

    for i in range(0,f+1):
        a = 7*(i+1)
        b = (-1)**(i)
        c = 10
        h(element, i, a,b,c)

    if returnResult: #when the stream is requesting the current result
        result = 0
        #[TODO]#
        #any additional processing to return the result at this point
        i = 0
        j = 0
        k = 5
        while i < f:
            # smooth Mean
            t = (memory1a[-i]+memory1a[-(i+1)])/2
            memory1a[j]=int(t)
            j+=1
            i+=1
        for i in range(k):
            t = 0
            s = float('inf')
            # inmemory sorting
            for j in range(i, k):
                if memory1a[j]<s:
                    s = memory1a[j]
                    t = j
            if s<memory1a[i]:
                memory1a[i], memory1a[t] = memory1a[t], memory1a[i]
        # median
        if k%2 == 0:
            result = (memory1a[k//2]+memory1a[(k//2)-1])//2
            if result>memory1a[6]:
                return memory1a[6]
        else:
            result = memory1a[k//2]
            if result>memory1a[6]:
                return memory1a[6]
        return result
    else: #no need to return a result
        pass


memory1b =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit
def task1BMedian(element, returnResult = True):
    ## please comment this block to use the other more efficient way.
    if memory1b[0]==None and memory1b[1]==None:
        memory1b[0] = 0
        memory1b[1] = 0

    memory1b[0]+=1
    memory1b[1]+=np.log(element)
    #


## please uncomment enitre block to run the new method(experiment)
## implement concept from below paper
### https://dl.acm.org/doi/pdf/10.1145/4372.4378
## Based on defining and maintaining 5 quantiles(0,25,50,75,100) of the distribution each time a new element is seen, returns the 50% percentile for median.
### Based on the concept that the 3 middle quantiles can be best approximated using 2nd order equation in our case a parabolic equation
## runs in limited time, gives accurate answers, please do consider
#     #[TODO]#
#     #process the element
#     if memory1b[-1]==None:
#         memory1b[-1]=0
#     memory1b[-1]+=1
#     if memory1b[-1] <=5:
#         memory1b[-memory1b[-1]-1]=element
#         if memory1b[-1]==5:
#             temp =[]
#             for i in range(2,7):
#                 temp.append(memory1b[-i])
#             temp.sort()
#             for i in range(2,7):
#                 memory1b[-i] = temp[i-2]
#             for i in range(5):
#                 memory1b[i]=i+1
#             for i in range(7,12):
#                 memory1b[-i] = i-6
#             del temp
#     else:
#         def sgn(o):
#             if o>=1:
#                 return 1
#             elif o<=-1:
#                 return -1
#         def parabolic(d,i):
#             t1 = memory1b[-i-1]
#             t2 = memory1b[-6-i-1]-memory1b[-6-i+1]
#             t3 = d/t2
#             t4 = memory1b[-6-i]-memory1b[-6-i+1]+d
#             t5 = (memory1b[-i-1-1] - t1)/(memory1b[-6-i-1] - memory1b[-6-i])
#             t6 = memory1b[-6-i-1] - memory1b[-6-i] - d
#             t7 = (t1 - memory1b[-i-1+1])/(memory1b[-6-i] - memory1b[-6-i+1])
#             final = t1 + t3*((t4*t5)+(t6*t7))
#             return final
#         def linear(d,i):
#             t1 = memory1b[-i-1]
#             t2 = memory1b[-i-1-d] - t1
#             t3 = memory1b[-6-i-d] - memory1b[-6-i]
#             final = t1 + d*(t2/t3)
#             return final
#         flag = 0
#         for j in reversed(range(7,12)):
#             if memory1b[-j]<=element:
#                 b = j
#                 flag = 1
#                 break
#         if flag==1:
#             for i in range(b,12):
#                 memory1b[-i]+=1
#         for i in range(5):
#             if i == 0:
#                 pass
#             elif i <4:
#                 memory1b[i] = i*0.25*(memory1b[-1]-1)+1
#             elif i==4:
#                 memory1b[i] = memory1b[-1]

#         for i in range(2,5):
#             dl = memory1b[i-1] - memory1b[-6-i]
#             if abs(dl)>=1:
#                 d = sgn(dl)
#                 qdash = parabolic(d, i)
#                 if memory1b[-i] < qdash < memory1b[-i-2]:
#                     flag2 = 0
#                     for m in range(7,11):
#                         if memory1b[-m]+d == memory1b[-m-1]:
#                             flag2 = 1
#                             break
#                     if flag2 == 0:    
#                         memory1b[-i-1] = qdash
#                         memory1b[-6-i]+=d
#                 else:
#                     flag2 = 0
#                     for m in range(7,11):
#                         if memory1b[-m]+d == memory1b[-m-1]:
#                             flag2 = 1
#                             break
#                     if flag2 == 0:    
#                         qdash = linear(d, i)
#                         memory1b[-i-1] = qdash
#                         memory1b[-6-i]+=d       

## last element of memory1b is used to count number of elements seen so far.
## 2nd to 6th position i.e. -2 to -6 is used to store qi's according to the paper.
## qi's are quantiles/percentiles: 0, 25%, 50%, 75%, 100% and number in each quantile denotes Highest number in the particular
### quantile seen so far
## 7 to 11 postion from last is used to store ni's (-7 to -11) in memory1b and this denotes number of elements observed so far 
### not greater than the current qi
## ni' 's(ni dash) are stored in the front i.e. 0 to 4 used to determine di's and therefore d.

        


    if returnResult: #when the stream is requesting the current result
        result = 0
        #[TODO]#
        #any additional processing to return the result at this point

        ## for Pareto distribution 
        result = 1/((0.5)**(memory1b[1]/memory1b[0]))
        if result - int(result) >=0.5:
            return int(result)+1
        else:
            return int(result)


        ## uncomment this for P2 Algorithm way
        # result = memory1b[-4]
        # return result
    else: #no need to return a result
        pass
    

memory1c =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit

def task1CMostFreqValue(element, returnResult = True):
    #[TODO]#
    #process the element
    # based on idea of storing top 5 elements and updating or replacing them if competition exists.
    if memory1c[0] == None:
        memory1c[0] = dict()
    if element in memory1c[0].keys():
        memory1c[0][element] += 1
    elif len(memory1c[0].keys())<5:
        memory1c[0][element]=1
    elif len(memory1c[0].keys())==5:
        for k,_ in memory1c[0].items():
            memory1c[0][k]-=1
            if memory1c[0][k]==0:
                del memory1c[0][k]

    
    if returnResult: #when the stream is requesting the current result
        result = 0
        #[TODO]#
        result = max(memory1c[0], key=memory1c[0].get)
        #any additional processing to return the result at this point
        return result
    else: #no need to return a result
        pass


##########################################################################
##########################################################################
# MAIN: the code below setups up the stream and calls your methods
# Printouts of the results returned will be done every so often
# DO NOT EDIT BELOW

def getMemorySize(l): #returns sum of all element sizes
    return sum([getsizeof(e) for e in l])+getsizeof(l)

if __name__ == "__main__": #[Uncomment peices to test]
    
    print("\n\nTESTING YOUR CODE\n")
    
    ###################
    ## The main stream loop: 
    print("\n\n*************************\n Beginning stream input \n*************************\n")
    filename = sys.argv[1]#the data file to read into a stream
    printLines = frozenset([10**i for i in range(1, 20)]) #stores lines to print
    peakMem = 0 #tracks peak memory usage
    
    with open(filename, 'r') as infile:
        i = 0#keeps track of lines read
        for line in infile:
        
            #remove \n and convert to int
            element = int(line.strip())
            i += 1
            
            #call tasks         
            if i in printLines: #print status at this point: 
                result1a = task1ADistinctValues(element, returnResult=True)
                result1b = task1BMedian(element, returnResult=True)
                result1c = task1CMostFreqValue(element, returnResult=True)
                
                print(" Result at stream element # %d:" % i)
                print("   1A:     Distinct values: %d" % int(result1a))
                print("   1B:              Median: %.2f" % float(result1b))
                print("   1C: Most frequent value: %d" % int(result1c))
                print(" [current memory sizes: A: %d, B: %d, C: %d]\n" % \
                    (getMemorySize(memory1a), getMemorySize(memory1b), getMemorySize(memory1c)))
                
            else: #just pass for stream processing
                result1a = task1ADistinctValues(element, False)
                result1b = task1BMedian(element, False)
                result1c = task1CMostFreqValue(element, False)
                
            memUsage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if memUsage > peakMem: peakMem = memUsage
        
    print("\n*******************************\n Stream Terminated \n*******************************")
    print("(peak memory usage was: ", peakMem, ")")

# for convinience
# python streamingCSE545sp22_Dayani_114241407.py test_incomes.csv
# python streamingCSE545sp22_Dayani_114241407.py trial_incomes.csv