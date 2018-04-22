#encoding:utf-8

import re
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
    time_1=time.time()
    samples = [list() for n in range(830)]
    print("len:%d"%len(samples))
    for i in range(len(sample_file)):
        if i>0:
            samples[int(sample_file[i][1])//100000].append(sample_file[i])
    time_2=time.time()
    for i in samples:
        print(len(i))
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
mutex=threading.Lock()
def mp_search_train(search_queue,searching_list,train_list_hash,search_result,timer):
    while 1:
        uid=search_queue.get()
        _hash=int(uid)//100000
        while 1:
            if not searching_list.count(_hash):
                searching_list.append(_hash)
                break
        if not len(train_list_hash[_hash]):
            continue;
        else:
            for i in range(len(train_list_hash[_hash])):
                _train=train_list_hash[_hash][i]
                if uid==_train[1]:
                    del train_list_hash[_hash][i]
                    searching_list.remove(_hash)
                    _train.append(uid)
                    search_result.append(_train)
                    _time=time.time()
                    print("time:%f"%(_time-timer))
                    timer=time.time()
                    print("found:%d"%len(search_result))
                    break

if __name__ == '__main__':

    userdata_vw_file='../preliminary_contest_data/userFeature.data'

    #ad_queue1=addata_read('../preliminary_contest_data/adFeature.csv')
    #ad_queue2=mp.Queue()
    #print('ad queue init size:')
    #print(ad_queue1.qsize())
    #print(len(ad_queue1))

    train_list_hash=trainsample_read('../preliminary_contest_data/train.csv')
    search_queue=mp.Queue(maxsize=50)
    searching_list=mp.Manager().list()

    userdata_file = open(userdata_vw_file)
    train_users_file = open('train_user.data','w')

    users = list()
    user = list()
    cpu_count=4

    print('initialization finished')

    search_result = mp.Manager().list()

    time_start = time.time()

    for j in range(50):
        p = mp.Process(target=mp_search_train, args=(search_queue,searching_list,train_list_hash,search_result,time_start,))
    p.start()
    while 1:
        line = userdata_file.readline()
        if not line:
            break

        uid = re.search(r"\s\d+", line)

        search_queue.put(uid.group())

    p.close()
    p.join()

    train_users_file.close()


