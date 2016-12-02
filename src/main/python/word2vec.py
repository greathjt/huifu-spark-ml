#!/usr/bin/env python
# encoding: utf-8

import vertica_python
import pandas as pd
import binascii
import jieba
from gensim.models import Word2Vec
import multiprocessing
import re
import time

t0=time.time()
#读入数据库数据
conn_info = {'host': '192.168.3.145','port': 5433,'user': 'etl_user','password': 'hfetl_user','database':
'chinapnrqasdb','ssl': False}
conn = vertica_python.connect(**conn_info)
data_check = pd.read_sql('''select filtered_plain_text from sky.public_opinion limit 90000''', conn)
#数据库16进制转文本，去掉特殊符号，将文本以list方式存储。list的每一个元素为一个文本。
r=re.compile(u'[’（）〈〉：．!"#$%&\'()*+,-./:;～<=>?@，。?↓★、�…【】《》？©“”▪►‘’！•[\\]^_`{|}~]+|[a-zA-Z0-9]')
corpus=[]
for i in range(len(data_check)):
    if data_check.ix[i,'filtered_plain_text']=='null':
        continue
    else:
        try:
            s=binascii.a2b_hex(data_check.ix[i,'filtered_plain_text'].replace(' ','')).decode("utf8")
            s=re.sub(r,"",s)
            corpus.append(s.replace('\n','').replace('\r','').replace('\t',''))
        except:
            continue
#分词
def sentence2words(paper):
    '''jieba分词，构建用户自定义词典user.txt'''
    #jieba.load_userdict(r'C:\Users\yuan.yuan\AppData\Local\Continuum\Anaconda3\Lib\site-packages\jieba\user.txt')
    seg_words = jieba.cut(paper,cut_all=False)
    words = [word for word in seg_words]
    return words
# list的每一个元素为一个文本，且文本经过分词转化。
def MySentences(paper_list):
    words=[]
    for paper in paper_list:
        words.append(sentence2words(paper))
    return words
#word2vec模型训练，并保存model文件和vector文件
def train_save(list_csv):
    sentences = MySentences(list_csv)
    model = Word2Vec(sentences, size=400, window=5, min_count=5,
            workers=multiprocessing.cpu_count())
    return model

if __name__ == "__main__":
    model = train_save(corpus)
    result = model.most_similar(u"警察",topn=20)
    for e in result:
        print(e[0])
