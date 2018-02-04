
# coding: utf-8

# In[6]:

import pyspark
import csv
import itertools
import re
import time

sc = pyspark.SparkContext()
doc_rdd = sc.textFile("sample/")
doc_rdd = doc_rdd.filter(lambda line: '>' in line)
data_rdd = doc_rdd.map(lambda line:(line.split('>')[0]+'>',line.split('>')[1]))
split_rdd = data_rdd.map(lambda (x,y): (x,y.replace("j","i").replace("v","u").split()))
reader = csv.reader(open('new_lemmatizer.csv'))
dictionary={}
for line in reader:
    l=[]
    for i in range(1,len(line)):
        if(line[i]!=''):
            l.append(line[i])
    dictionary[line[0]]=l   
def checkd(word):
    l=[]
    word = re.sub('[^A-Za-z0-9]+', '', word)
    if word in dictionary:
        return dictionary[word]
    else:
        l.append(word)
        return l
def matchwordpairs(x,y):
    l=[]
    size = len(y)
    for i in range(0,size-1):
        for z in list(itertools.product(checkd(y[i]),checkd(y[i+1]))):
                l.append((z,x))
    return l

dict_rdd = split_rdd.map(lambda (x,y): (matchwordpairs(x, y)))
t = time.time()
dict_rdd.flatMap(lambda x:x).reduceByKey(lambda x,y:x+y).saveAsTextFile("sample_op_wp/" + str(t))
sc.stop()


# In[ ]:



