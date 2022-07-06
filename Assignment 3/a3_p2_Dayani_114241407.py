import pyspark
import sys
import json 
from datetime import datetime
import numpy as np
import math


sc = pyspark.SparkContext()
filename = sys.argv[1]
data_rdd = sc.textFile(filename)

tar = ['PomQayG1WhMxeSl1zohAUA','uEvusDwoSymbJJ0auR3muQ', 'q6XnQNNOEgvZaeizUgHTSw','n00anwqzOaR52zgMRaZLVQ','qOdmye8UQdqloVNE059PkQ']
tp = data_rdd.map(lambda x: json.loads(x))
task211 =  tp.map(lambda x: ((x['user_id'], x['business_id']),(x['stars'],x['date'])))
task212 = task211.reduceByKey(lambda x,y: x if x[1][1]>=y[1][1] else y)\
    .map(lambda x: (x[0][1],((x[0][0],x[1][0]),),))
task213 = task212.reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1])>=30) 
task214 = task213.flatMap(lambda x: [(user[0], ((x[0], user[1]),),) for user in list(x[1])]).reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1]) >= 5)
task215 = task214.filter(lambda x: x[0] in tar)
chk1 = task215.collect()

print('first 10 business_ids and ratings for the target users:','\n')
for item in chk1:
    print("target:",item[0],"business:",sorted(item[1][:10]))

print('\n','\n')


#############################################################################################################################################
target = task215.collect()

def tar2(bs):
    com = []
    for item in target:
        count = 0
        for busi in item[1]:
            for xb in bs:
                if xb[0] == busi[0]:
                    count+=1
                    break
            if count >= 2:
                com.append(item[0])
                break
    return tuple(com)
    

task221_222a = task214.map(lambda x: (x[0],(x[1],tar2(x[1])))).filter(lambda x: len(x[1][1])>=2)\
    .flatMap(lambda x:[(item, ((x[0], x[1][0]),)) for item in x[1][1]])#


def meanc(lis):
    val = []
    for item in lis:
        val.append(item[1])
    mean = np.mean(val)
    for i in range(len(lis)):
        lis[i] = list(lis[i])
        lis[i][1]-=mean
    return lis

def cos(u,t):
    num = 0
    for i1 in u:
        for i2 in t:
            if i1[0] == i2[0]:
                num+= (i1[1]*i2[1])
    a,b = 0,0
    for i in u:
        a+= (i[1]**2)
    for i in t:
        b+= (i[1]**2)
    denom = np.sqrt(a)*np.sqrt(b)
    ans =  num/denom
    if not(math.isnan(float(ans))):
        return ans
    else:
        return 0
            
targetmc = task215.map(lambda x: (x[0], meanc(list(x[1]))))
t1 = task221_222a.map(lambda x: (x[0],(x[1][0][0],meanc(list(x[1][0][1])),)))
tar_mc = t1.join(targetmc).map(lambda x: (x[0],(( x[1][0][0] , cos(x[1][0][1],x[1][1]) ),))).filter(lambda x: x[1][0][1]>0 and x[0]!=x[1][0][0]).sortBy(lambda x: x[1][0][1], False)
task222 = tar_mc.reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], x[1][:50]))



task223s = task222.flatMap(lambda x: [(i[0], (x[0],i[1],))for i in x[1]])
# print(task223s.take(50))
nrdd = tp.map(lambda x: (x['user_id'],(x['business_id'],x['stars'],)))
joinrdd = task223s.join(nrdd).map(lambda x: ((x[1][0][0],x[1][1][0]),(x[1][0][1]*x[1][1][1],x[1][0][1],1)))
task223 = joinrdd.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1],x[2]+y[2])).filter(lambda x:x[1][2]>=3).map(lambda x: (x[0][0],((x[0][1],x[1][0]/x[1][1]),)))\
.reduceByKey(lambda x,y:x+y)

ck22 = task223.collect()
print("#############User-User Collabrorative Filtering###############",'\n')
for item in ck22:
    print("Target User:",item[0],'\n')
    br = sorted(item[1])
    for i in range(10):
        print("Business ID:", br[i][0])
        print("Predicted Rating:", br[i][1],'\n')


############################################################################################################################################
# #part 2_3
target_b = task215.flatMap(lambda x: [(x[0],(i[0],)) for i in x[1]]).reduceByKey(lambda x,y: x+y).collect()
    #.reduceByKey(lambda x,y: x+y).map(lambda x:x[1]).reduce(lambda x,y: x+y )
dic = dict((x, y) for x, y in target_b)
# print(dic)
def tar3(lis):
    for item in lis:
        t_i = set()
        if item[0] in dic.keys():
            for i in dic[item[0]]:
                t_i.add(i)
    return list(t_i)
task231 = task214.flatMap(lambda x: [(i[0], ((x[0], i[1]),))for i in x[1]]).reduceByKey(lambda x,y:x+y)\
    .map(lambda x: (x[0],(x[1], tar3(x[1])))).filter(lambda x: len(x[1][1])>0)


task232 = task231.flatMap(lambda x:[(i, ((x[0], x[1][0]),))for i in x[1][1]]).map(lambda x:(x[0], (x[1][0][0], meanc(list(x[1][0][1])),)))
n215 = task215.flatMap(lambda x:[(i[0], ((x[0], i[1]),)) for i in x[1]]).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], meanc(list(x[1]))))
t2 = task232.join(n215).map(lambda x: (x[0], ((x[1][0][0] , cos(x[1][0][1],x[1][1])),))).filter(lambda x: x[1][0][1]>0 and x[0]!=x[1][0][0])
task232f = t2.reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], x[1][:50])) ######
# # print(task232f.collect())

tar33 = task215.flatMap(lambda x: [(i[0], (x[0],i[1],))for i in x[1]])
inp = task232f.flatMap(lambda x: [(x[0], (i[0], i[1])) for i in x[1]])
task233 = inp.join(tar33).map(lambda x: ((x[1][1][0],x[1][0][0]),(x[1][0][1]*x[1][1][1], x[1][0][1],1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1],x[2]+y[2]))\
    .filter(lambda x:x[1][2]>=3).map(lambda x: (x[0][0],((x[0][1],x[1][0]/x[1][1]),))).reduceByKey(lambda x,y:x+y)

ck23 = task233.collect()
print("#############Item-Item Collabrorative Filtering###############",'\n')
for item in ck23:
    print("Target User:",item[0],'\n')
    br = sorted(item[1])
    for i in range(10):
        print("Business ID:", br[i][0])
        print("Predicted Rating:", br[i][1],'\n')





