import sys
import pyspark
import csv
import random
import hashlib
import numpy as np
# import time
random.seed(42)

sc = pyspark.SparkContext()
val = ['150034', '050739', '330231', '241326', '070008']
val_bc = sc.broadcast(val)
def task2a(data):
    def util(lis):
        res = set()
        for i in range(1,len(lis)):
            if lis[i] != '':

                res.add(str(header_rdd.value[i-1])+":"+str(lis[i]))
        return res

    header = data.first()
    data_rdd = data.filter(lambda x: x!=header).mapPartitions(lambda x: csv.reader(x))
    head = sc.parallelize(header.split(",")).collect()
    header_rdd = sc.broadcast(head[1:])
    union_rdd = data_rdd.map(lambda x:(x[0],util(x))).reduceByKey(lambda a,b: a.union(b))
    filter_rdd = union_rdd.filter(lambda x: x[0] in val_bc.value).collect()
    ## complete filter part
    # print(union_rdd.take(2)[1]) #5079
    # print(new_rdd.count()) #419754
    return union_rdd,filter_rdd

def task2b(data):
    def hash(a,b,x):
        return (a*x+b)%99991
    hash_lis = [(i,(6*int(random.random()*100),((-1)**i)*5*(int(random.random()*100)))) for i in range(100)]
    hash_bc = sc.broadcast(hash_lis)

    def util2(x):
        lis = []
        for hi in hash_bc.value:
            minval = np.inf
            for s in x[1]:
                str_hash = int(str(hashlib.md5(s.encode()).hexdigest()),16)
                minval = min(minval, hash(hi[1][0],hi[1][1],str_hash))
            lis.append(((x[0],hi[0]),minval))
        return lis

    m = data.flatMap(lambda x: util2(x))#
    filter_rdd = m.filter(lambda x: x[0][0] in val_bc.value).map(lambda x: (x[0][0],[x[1]])).reduceByKey(lambda x,y: x+y).collect()#l
    return m,filter_rdd
    

def task2c(data):
    def hash(a,b,x):
        return (a*x+b)%1009
    hash_lis = [(i,(6*int(random.random()*100),((-1)**i)*5*(int(random.random()*100)))) for i in range(20)]
    hash_bc = sc.broadcast(hash_lis)
    b_rdd = data.map(lambda x:((x[0][0],x[0][1]//20), x[1])).reduceByKey(lambda x,y: x+y)\
    .map(lambda x:((x[0][1],hash(hash_bc.value[x[0][1]][1][0],hash_bc.value[x[0][1]][1][1],x[1])),[x[0][0]]))
    m = b_rdd.reduceByKey(lambda x,y: x+y)
    dic = {}
    for item in val_bc.value:
        fil = m.filter(lambda x: item in x[1]).map(lambda x:set(x[1])).reduce(lambda x,y: x.union(y))
        fil.remove(item)
        dic[item] = fil
    # final_rdd = sc.parallelize(dic.items())
    # jaccard_rdd = data.filter(lambda x: (x[0][0] in dic.values()))
    jaccard = []
    task2_3 = []
    for k,v in dic.items():
        # print(k,v)
        key = data.filter(lambda x: x[0][0] == k).map(lambda x: [x[1]]).reduce(lambda x,y: x+y)
        # print(key)
        val = data.filter(lambda x: x[0][0] in v).map(lambda x: (x[0][0],[x[1]])).reduceByKey(lambda x,y:x+y).collect()
        for value in val:
            # print(key)
            res = len(set(key).intersection(set(value[1])))/len((set(key).union(set(value[1]))))
            jaccard.append((k,value[0],res))
            task2_3.append(((k,key[:10]),(value[0],value[1][:10])))
        
    return dic,jaccard,task2_3

if __name__ == "__main__":
    # start = time.time()
    hospitals_rdd = sc.textFile(sys.argv[1],32)
    input2b_rdd,task1 = task2a(hospitals_rdd)
    input2c_rdd,task2=task2b(input2b_rdd)
    final,j,task2_3_3 = task2c(input2c_rdd)
    for item in task1:
        print("Hospital ID: ", item[0],'\n','Hospital Feautres: ', item[1],'\n')
    print('\n''\n''\n''\n''\n')
    for item in task2:
        print("Hospital ID: ", item[0],'\n','Signature Matrix: ', item[1],'\n')
    print('\n''\n''\n''\n''\n')
    for k, v in final.items():
        print("Hospital ID: ", k, '\n','Similar Hospitals: ', v)
    print('\n''\n''\n''\n''\n')
    for item in j:
        print("Hospital ID: ", item[0], '\t', "Similar Hospital: ", item[1], '\t', "Jaccard Similarity: ", item[2] )
    print('\n''\n''\n''\n''\n')
    for item in task2_3_3:
        print("Hospital ID: ", item[0][0],'\t', "Signatures: " ,item[0][1] ,'\n',"Similar Hospital ID: ", item[1][0],'\t', 'Signatures: ', item[1][1],'\n''\n')
    # end= time.time()
    # print(end-start)