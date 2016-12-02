#!/usr/bin/env python
# encoding: utf-8

from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.mllib.feature import Word2Vec
import binascii
import jieba
import re

conf = SparkConf().setAppName('Spark').setMaster('local')
sc = SparkContext(conf=conf)
file_path="hdfs://Ucluster/sky/output/20161119/BD_PAGE_REPOSITORY/result-m-00028"
sqlContext = SQLContext(sc)
#读取HDFS文件，创建为rdd
rdd=sc.textFile(file_path)
rdd = rdd.map(lambda line: line.split("|"))
#取第8列filteredPlainText，创建为spark的datafram
df = rdd.map(lambda line: Row(filteredPlainText=line[8])).toDF()
#spark的dataframe转化为pandas，方便后续数据清洗
data_check=df.toPandas()
#将filteredPlainText16进制转文本，去掉特殊符号，并且分词
r=re.compile(u'[’（）〈〉：．!"#$%&\'()*+,-./:;～<=>?@，。?↓★、�…【】《》？©“”▪►‘’！•[\\]^_`{|}~]+|[a-zA-Z0-9]')
for i in range(len(data_check)):
    try:
        s=binascii.a2b_hex(data_check.ix[i,'filteredPlainText'].replace(' ','')).decode("utf8")
        s=re.sub(r," ",s)
        paper=s.replace('\n','').replace('\r','').replace('\t','')
        data_check.ix[i,'filteredPlainText']=" ".join(jieba.cut(paper,cut_all=False))
    except:
        continue
#pandas转为spark dataframe
spdf=sqlContext.createDataFrame(data_check)
#spark dataframe转为rdd
rdd=spdf.rdd
#去掉rdd的row标签，转为word2vec要求的输入格式
t=rdd.map(list)
a=t.map(lambda row: "".join(row))
input=a.map(lambda row: row.split(" "))
#训练word2vec
word2vec = Word2Vec()
model = word2vec.fit(input)
#输出近义词（前20个）和距离
synonyms = model.findSynonyms('理财', 20)
for word, cosine_distance in synonyms:
    print("{}: {}".format(word, cosine_distance))