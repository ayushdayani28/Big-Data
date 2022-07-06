import pyspark
import sys
# import time

sc = pyspark.SparkContext()
def task1a(data): 
    dis_incomes = data.distinct().count()
    return dis_incomes

def task1b(data, l):
    sorted_data = data.sortBy(lambda x : x).zipWithIndex()
    if l%2 != 0:
        k = sorted_data.filter(lambda x: x[1] == l//2).collect()
        return k[0][0]
    else:
        k = sorted_data.filter(lambda x: x[1]==l//2 or x[1]==(l//2+1)).collect()
        final = (k[0][0]+k[1][0])/2
        return final

def task1c(data):
    count = data.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)
    res = count.max(lambda x : x[1])
    return res[0]

def task1d(data):
    cp10_RDD = data.map(lambda x: ((10**(len(str(x))-1)),1)).reduceByKey(lambda a,b: a+b)
    res = sorted(cp10_RDD.collect())
    return res

if __name__ == "__main__":
    # start = time.time()
    filename = sys.argv[1]
    data_main = []
    with open (filename, 'r') as infile:
        for line in infile:
            data_main.append(int(line.strip()))
    data = sc.parallelize(data_main)
    d_incomes = task1a(data)
    median = task1b(data, len(data_main))
    mode = task1c(data)
    cp_10 = task1d(data)
    # end = time.time()
    print("Distinct Incomes: ",d_incomes,'\n',
            "Median: ", median, '\n',
            "Mode: ",mode, '\n',
            'Count per 10 Power: ', cp_10, '\n')
    # print(end-start)