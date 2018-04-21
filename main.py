#encoding:utf-8

import re
import pandas as pd
import csv
import time
import threading
import multiprocessing as mp
import os
from multiprocessing import Pool
import threading
import queue

def userdata_read( userdata_vw_file ,nlines):
    userdata_file = open( userdata_vw_file )
    users = list()
    user = list()
    for i in range(nlines):
        line = userdata_file.readline()
        features = re.findall(r"[a-zA-Z0-9\s]+[\|\n]",line)
        for feature in  features:
            feature_name = re.findall(r"[a-zA-z]+\d?",feature);
            feature_values = re.findall(r"\s\d+",feature);
            for feature_value in feature_values:
                feature_value = int(feature_value)
            user.append([feature_name,feature_values])
        users.append(user)
    return users;

def addata_read( addata_csv_file ):
    addata_file = list(csv.reader(open(addata_csv_file)))
    '''
    ads = mp.Queue();
    ad = list()
    feature_names = list()
    for tmpad in addata_file:
        if addata_file.index(tmpad)==0:
            for name in tmpad:
                feature_names.append(name)
        else:
            for feature_value in tmpad:
                #print(feature_value)
                #print(tmpad)
                ad.append([feature_names[tmpad.index(feature_value)],feature_value])
            ads.put(ad)
    print(ads.qsize())
    '''

    ads = list()
    ad = list()
    feature_names = list()
    for tmpad in addata_file:
        if addata_file.index(tmpad)==0:
            for name in tmpad:
                feature_names.append(name)
        else:
            for feature_value in tmpad:
                #print(feature_value)
                #print(tmpad)
                ad.append([feature_names[tmpad.index(feature_value)],feature_value])
            ads.append(ad)
    print(len(ads))
    return ads;

def trainsample_read( trainsample_csv_file ):
    sample_file = list(csv.reader(open(trainsample_csv_file)))
    '''
    samples = mp.Queue();
    for i in range(len(sample_file)):
        if i>0:
            samples.put(sample_file[i])
    print(samples.qsize())
    '''
    test = mp.Queue();

    time_1=time.time()
    samples = list();
    for i in range(len(sample_file)):
        if i>0:
            samples.append(sample_file[i])
    time_2=time.time()
    print(len(samples))
    print('list time:%f'%(time_2-time_1))

    return samples;
'''
def mp_search_train(uid,queue_id,train_queue1,train_queue2,mtched_train):
    if queue_id:
        train_queue_out=train_queue2;
        train_queue_in=train_queue1;
    else :
        train_queue_out=train_queue1;
        train_queue_in=train_queue2;
    while 1:
        if not train_queue_out.empty():
            #print('founding')
            tmp_train=train_queue_out.get_nowait()
            if not mtched_train:
                if uid==tmp_train[1]:
                    mtched_train=tmp_train;
                    print('found')
                else:
                    train_queue_in.put_nowait(tmp_train)
            else:
                train_queue_in.put_nowait(tmp_train)
        else:
            print('not found')

def mp_search_ad(aid,queue_id,mtched_ad):
    global ad_queue
    if queue_id:
        queue_id2=0
    else :
        queue_id2=1
    while 1:
        if not ad_queue[queue_id].empty():
            tmp_ad=ad_queue[queue_id].get()
            if not mtched_ad:
                if aid==tmp_ad[0][1]:
                    mtched_ad=tmp_ad;
            ad_queue[queue_id2].put(tmp_ad)
            ad_queue[queue_id].task_done()
        else:
            return ;
'''
def mp_search_train(uid,train_list,ind,length,mtched_train):
    while 1:
        id=ind
        ind=ind+1
        if mtched_train:
            break
        if id<length:
            if uid==train_list[id][1]:
                mtched_train=train_list[id][1]
                del train_list[id]
                break
        else:
            break

if __name__ == '__main__':

    userdata_vw_file='../preliminary_contest_data/userFeature.data'

    ad_queue1=addata_read('../preliminary_contest_data/adFeature.csv')
    ad_queue2=mp.Queue()
    print('ad queue init size:')
    #print(ad_queue1.qsize())
    print(len(ad_queue1))

    train_queue1=trainsample_read('../preliminary_contest_data/train.csv')
    train_queue2=mp.Queue()
    print('train queue init size:')
    #print(train_queue1.qsize())
    print(len(train_queue1))

    userdata_file = open(userdata_vw_file)
    train_users_file = open('train_user.data','w')

    users = list()
    user = list()
    cpu_count=4
    #search_train_pool=Pool(cpu_count)
    #search_ad_pool=Pool(mp.cpu_count())

    n=0

    print('initialization finished')

    mtched_train = mp.Manager().list()
    mtched_ad = mp.Manager().list()
    ind=mp.Value("d",0)
    length=mp.Value("d",len(train_queue1))
    for i in range(100):
        print("processing :%d"%i)
        time_start=time.time()
        line = userdata_file.readline()
        if not line:
            break

        mtched_train=[]
        mtched_ad=[]

        uid = re.search(r"\s\d+", line)

        for j in range(50):
            p=mp.Process(target=mp_search_train, args=(uid.group(), train_queue1,ind,
                                                       length,mtched_train,))

        p.start();
        p.join()

        '''
        for j in range(len(train_queue1)):
            if uid.group()==train_queue1[j][1]:
                mtched_train=train_queue1[j][1]
                del train_queue1[j]
        '''

        if mtched_train:
            train_users_file.write(line)
            '''
            for j in range(mp.cpu_count()):
                search_ad_pool.apply_async(mp_search_ad, args=(mtched_train[0], i % 2))
    
            search_ad_pool.close()
            search_ad_pool.join()
            '''
            print('processing:')
            n+=1
            print(n)

        '''
        for j in range(len(train_samples)):
            train_sample=train_samples[j]
            print(feature_values)
            #print(train_sample[1])
           # if features[0][1]==train_sample[1]:
            if 1:
                train_users_file.write(line)
                del train_samples[j]
                break;
        '''
        time_end=time.time();
        print('time:%s'%(time_end-time_start))
    train_users_file.close();

    '''
    for feature in features:
        feature_name = re.findall(r"[a-zA-z]+\d?", feature);
        feature_values = re.findall(r"\s\d+", feature);
      #  for feature_value in feature_values:
      #      feature_value = int(feature_value)
        user.append([feature_name, feature_values])
    '''


