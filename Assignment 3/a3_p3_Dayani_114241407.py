import pyspark
import json
import sys
# import time
import csv
import numpy as np
from scipy.stats import t
from statistics import mean 

# start = time.time()

sc = pyspark.SparkContext()
f1 = sys.argv[1]
f2 = sys.argv[2]
f3 = sys.argv[3]
f4 = sys.argv[4]

f1_rdd = sc.textFile(f1)
f2_rdd = sc.textFile(f2)
f3_rdd = sc.textFile(f3)
f4_rdd = sc.textFile(f4)

zips = ['89109', '89118', '15237', '44122', '44106']
header = f3_rdd.first()
ind = lambda x: header.split(',').index(x)
data_rdd = f3_rdd.filter(lambda x: x!=header).mapPartitions(lambda x: csv.reader(x))
req_rdd = data_rdd.map(lambda x:(x[ind('hospital_pk')],(x[ind('total_beds_7_day_avg')],x[ind('inpatient_beds_used_7_day_avg')],x[ind('zip')])))
task311 = req_rdd.filter(lambda x: x[1][0] and x[1][1] and float(x[1][0])>=30 and x[1][1]!='-999999')
task312 = task311.map(lambda x: ((x[0], x[1][2]),float(x[1][1])/float(x[1][0]))).groupByKey().map(lambda x: (x[0][1],mean(x[1])))
task313 = task312.groupByKey().map(lambda x:(x[0],mean(x[1])))
check1 = task313.filter(lambda x: x[0] in zips)

print("The mean_bed_usage_pct for zip codes: 89109, 89118, 15237, 44122, 44106")
for item in check1.collect():
    print("ZIP:",item[0])
    print("mean_bed_usage_pct:", item[1])

print('\n')


#############################################################################################################################################

r_rdd = sc.textFile(f1).map(lambda x:json.loads(x)).map(lambda x: (x['business_id'], x['text']))
b_rdd = sc.textFile(f2).map(lambda x:json.loads(x)).map(lambda x: (x['business_id'], x['postal_code']))
task32_12_rdd = r_rdd.leftOuterJoin(b_rdd).filter(lambda x: (x[1][1] in zips) and (len(x[1][0])>=256)).map(lambda x: (x[1][1], [x[1][0]])).reduceByKey(lambda x,y: x+y)
# print(task32_12_rdd.count())
with open(f4, newline='') as f:
    reader = csv.reader(f)
    dictionary = list(reader)

dic = []
for item in dictionary:
    dic.append(item[0])
dic_bc = sc.broadcast(dic)


def check(s):
    temp = dict.fromkeys(dic_bc.value, 0)
    # s = s.split()
    l = len(s)
    for i in s:
        k = sorted(list(set(i.split(" "))))
        for item in dic_bc.value:
            for char in k:
                if item[-1] == '*' and  char.lower()<=item[:-1].lower()+'~' and char.startswith(item [:-1]) :
                    temp[item] += 1
                    break
                elif item[-1] == '*' and char.lower()>item[:-1].lower()+'~':
                    break
                elif item[-1]!='*' and char.lower()<=item.lower() and char == item:
                    temp[item] += 1
                    break
                elif item[-1]!='*' and char.lower()>item.lower():
                    break
    for k,_ in temp.items():
        temp[k] /= l

    temp = sorted(temp.items(), key=lambda x: x[1], reverse=True)

    return temp 


task323_rdd = task32_12_rdd.map(lambda x: (x[0], check(x[1])))
task323 = task323_rdd.map(lambda x:(x[0], x[1][:5]))
print("5 most frequent words, along with their usage_score for zip codes: 89109, 89118, 15237, 44122, 44106")
print(task323.collect())

print('\n')
###############################################################################################################

def var(vals):
    X, Y = [], []
    for x,y in vals:
        if(x != None and y != None):
            X.append(x)
            Y.append(y)
    Xm = sum(X)/len(X)
    Ym = sum(Y)/len(Y)
    corr = sum((a - Xm) * (b - Ym) for (a,b) in zip(X,Y)) / len(Y)
    r = corr / (np.std(X) * np.std(Y))
    t_stat = r*np.sqrt((len(X)-2)/(1-(r**2)))
    p = t.sf(abs(t_stat), df=len(X)-2)
    p_corrected = p/len(dic_bc.value)
    return {'corr':r, 'p-value':p, 'c_p': p_corrected}

p_corr, n_corr = [],[]
dic = {}

corr = task323_rdd.leftOuterJoin(task313).flatMap(lambda x: ((i[0], [(i[1], x[1][1])]) for i in x[1][0]))\
    .reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], var(x[1]))).collect()

for k,v in corr:
    dic[k] = v
    if (v['corr'] != np.nan and v['corr']>=0):
        p_corr.append((v['corr'], k))
    elif (v['corr'] != np.nan and v['corr']< 0):
        n_corr.append((v['corr'], k))

p_corr.sort(reverse=True)
n_corr.sort()
pos = p_corr[:20]
neg = n_corr[:20]

p_res,n_res = [],[]

for k,v in pos:
    p_res.append((v, dic[v]))
for k,v in neg:
    n_res.append((v, dic[v]))


print("Highest 20 positive correlated values: ",p_res, '\n')
print("Lowest 20 negative correlated values: ",n_res, '\n')


# end = time.time()
# print(end-start)

