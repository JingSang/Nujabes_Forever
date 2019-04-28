import pandas as pd
import numpy as np
from tqdm import *
from datetime import *
import time
import math
from sklearn import preprocessing
import matplotlib.pyplot as plt



"""
@brief:获取部分字段缺失的数据保存在part_of_the_loss_data.csv中
@process:
"""
def save_Part_of_the_loss_data():
    for i in tqdm(range(1, 34)):
        data = get_data(i, 'inner')
        sub = pd.DataFrame(data)
        res = pd.read_csv('template_submit_result.csv', parse_dates=[0])[['ts', 'wtid']]
        DF = res.merge(sub, on=['wtid', 'ts'], how='inner')
        if i == 1:
            DF.to_csv('part_of_the_loss_data.csv', mode='a', index=False)
        else:
            DF.to_csv('part_of_the_loss_data.csv', mode='a', index=False, header=False)

"""
@brief:获取完整的没有任何缺失的数据集作为训练集--保存在train_dataset目录下
@process:
"""
def save_full_data(i):
    file = open('dataset/' + str(i).zfill(3) + '/201807.csv', 'r')
    lines = file.readlines()
    row = []  # 定义行数组
    c=0
    for line in lines:
        flag = True
        arr=line.replace('\n','').split(',')
        for j in range(3,len(arr)):
            #print(arr[i])
            if len(arr[j])==0:
                flag=False
                c=j
                break
        if flag==True:
            row.append(arr)
    data = pd.DataFrame(row)
    print('train_dataset/' + str(i).zfill(3) + '/dataset.csv')
    data.to_csv('train_dataset/' + str(i).zfill(3) + '/dataset.csv',header=False,index=False)
    file.close()

"""
@brief: 获取每个风机的所有数据：包括三部分--完整的没有缺失的数据，部分缺失的数据，完全缺失的数据
@process:
@:return: 返回每个风机的所有数据
"""
def get_data(i,method):
    start = time.clock()
    data = pd.read_csv('dataset/' + str(i).zfill(3) + '/201807.csv', parse_dates=[0])
    #读取template_submit_result文件,ts,wtid为读取对应csv文件的列
    res = pd.read_csv('template_submit_result.csv', parse_dates=[0])[['ts', 'wtid']]
    data = res.merge(data, on=['wtid', 'ts'], how=method)
    data=data[data['wtid'].isin([i])]
    data = data.sort_values(['wtid', 'ts']).reset_index(drop=True)
    elapsed = (time.clock() - start)
    print("Time used:", elapsed)
    #data.to_csv('all_data.csv',index=False)
    return data



"""
@brief:  利用python插值算法将缺失的数据进行修复--将那些修复的数据保存在python_interpolate_data.csv文件中
@process:
"""
def  save_interpolate_data():
    for i in tqdm(range(1,2)):

        data = get_data(i,'outer')
        sub = pd.DataFrame(data)
        #看特征分布  不同的特征用不同的插值
        fe = [i for i in data.columns if 'var' in i]
        for j in fe:
            if j in('var016','var020','var047','var053','var066'):
                data[j] = data[j].interpolate(method='nearest', axis=0)
            else:
                data[j]=data[j].interpolate(method='linear', axis=0).round(decimals=2)
        data.to_csv('polynomial_data.csv',index=False)
        print('ok')
        sub["var016"] = sub["var016"].astype(int)
        sub["var020"] = sub["var020"].astype(int)
        sub["var047"] = sub["var047"].astype(int)
        sub["var053"] = sub["var053"].astype(int)
        sub["var066"] = sub["var066"].astype(int)
        res = pd.read_csv('template_submit_result.csv', parse_dates=[0])[['ts', 'wtid']]
        DF = res.merge(sub, on=['wtid', 'ts'], how='inner')
        if i==1:
            DF.to_csv('python_interpolate_data.csv', mode='a', index=False)
        else:
            DF.to_csv('python_interpolate_data.csv', mode='a', index=False,header=False)

"""
@brief:  将部分和完全的缺失数据获取出来-保存在一个all_miss_data.csv文件中
@process:
"""
def save_all_miss_data():
    for i in tqdm(range(1, 2)):
        data = get_data(i, 'outer')
        sub = pd.DataFrame(data)
        res = pd.read_csv('template_submit_result.csv', parse_dates=[0])[['ts', 'wtid']]
        DF = res.merge(sub, on=['wtid', 'ts'], how='inner')
        if i==1:
            DF.to_csv('all_miss_data.csv', mode='a', index=False)
        else:
            DF.to_csv('all_miss_data.csv', mode='a', index=False,header=False)

"""
@brief:
@process:
"""
def save_all_miss_data_to_directer():
    for i in tqdm(range(1, 20)):
        data = get_data(i, 'outer')
        sub = pd.DataFrame(data)
        res = pd.read_csv('template_submit_result.csv', parse_dates=[0])[['ts', 'wtid']]
        DF = res.merge(sub, on=['wtid', 'ts'], how='inner')
        DF.to_csv('miss_dataset/' + str(i).zfill(3) + '/miss_data.csv', mode='a', index=False)

#将这两个文件进行join
#对于完全确实的数据--从sub_DCIC.csv文件中获取
#对于部分缺失的数据从sub_DCIC_MissData.csv文件中获取
#将这两部分数据组成一个新的文件


"""
@brief: 获取已经被插值之后的部分缺失数据
@process:
"""
def save_interpolated_part_miss_data():
    df=pd.DataFrame()
    total_data = pd.read_csv('sub_DCIC.csv', parse_dates=[0])
    # 读取template_submit_result文件,ts,wtid为读取对应csv文件的列
    fe = [i for i in total_data.columns if 'var' in i or 'ts' in i or 'wtid' in i]
    part_miss_data = pd.read_csv('sub_DCIC_part_MissData.csv', parse_dates=[0])
    DF = total_data.merge(part_miss_data, on=['wtid', 'ts'], how='inner',suffixes=('','_x'))
    for j in fe:
        df=pd.concat([df,DF[j]],axis=1)
    print('save:predict_data.csv')
    df.to_csv('predict_data.csv')

"""
@brief:获取已经被插值之后的完全缺失数据
"""
def save_interpolated_full_miss_data():
    df = pd.DataFrame()
    total_data = pd.read_csv('sub_DCIC.csv', parse_dates=[0])
    fe = [i for i in total_data.columns if 'var' in i or 'ts' in i or 'wtid' in i]
    part_miss_data = pd.read_csv('sub_DCIC_part_MissData.csv', parse_dates=[0])
    DF = total_data.merge(part_miss_data, on=['wtid', 'ts'], how='inner', suffixes=('', '_x'))
    for j in fe:
        df = pd.concat([df, DF[j]], axis=1)
    print('save:predict_data.csv')
    df.to_csv('predict_data.csv',index=False)

"""
@brief:将预测后的数据和插值的数据合并
"""
def merge_data():
    df=pd.DataFrame()
    miss_data = pd.read_csv('all_miss_data.csv', parse_dates=[0])
    predict_data=pd.read_csv('finalresult.csv',parse_dates=[0])
    fe = [i for i in miss_data.columns if 'var' in i]
    for i in fe:
        print(i)
        miss_tmp_x = miss_data[miss_data[i].isnull() == True]
        miss_index = miss_tmp_x.index.values
        print(len(miss_index))
        for j in range(0,len(miss_index)):
            miss_data.loc[[miss_index[j]], [i]] = predict_data.loc[[miss_index[j]], [i]]
            # print(miss_data.loc[[miss_index[j]], [i]])
    miss_data.to_csv("final_merge_result.csv",index=False)




    #DF=total_data.merge(predict_data,on=['wtid', 'ts'], how='inner', suffixes=('', '_x'))


"""
获取插值的x和y,以及缺失值的x,x为当前数据的时间与数据集第一条的数据的时间的时间差，y为需要插值维度的对应的维度值。
row_data:完整的没有缺失的数据集
miss_data:缺失的数据集
row_x: 将原始数据的时间，转为秒
miss_tmp_x:获取缺失值的对应的时间
miss_index:缺失值在数据集对应的位置
miss_x:将缺失值对应的时间转化为秒，并且减去数据集中第一条数据的秒数
x:将row_x，每一条数据减去数据集中第一条数据的秒数
y:x对应的维度值
index:使用切比雪夫取值法。对应的点的位置。
re_index:找出离切比雪夫点最近的x的点。
批量插值
"""
def load_data_to_get_xy(i,row_data,miss_data):
    row_x=[t.value//10**9 for t in row_data['ts']]
    miss_tmp_x=miss_data[miss_data['var'+str(i).zfill(3)].isnull()==True]
    miss_index=miss_tmp_x.index.values
    miss_x=[t.value//10**9-row_x[0] for t in miss_tmp_x['ts']]

    x=[i-row_x[0] for i in row_x]
    y = row_data.loc[:,'var'+str(i).zfill(3)].values
    index = [ int((np.max(x)/2)*(math.cos(((2*i-1)*math.pi)/(2*30))+1)) for i in range(1,31)]
    j=0
    index=sorted(index)
    re_index=[]
    for i in range(0,len(index)):
        if j>=len(x):
            break
        while x[j]<index[i]:
            j=j+1
        if (x[j] >=index[i]):
            re_index.append(x[j])
            index[i]=j
            j = j + 1
    y=[y[i] for i in index]
    return re_index,y,miss_x,miss_data,miss_index


def load_data_to_get_xy1(i, row_data, miss_data):
    row_x = [t.value // 10 ** 9 for t in row_data['ts']]
    miss_tmp_x = miss_data[miss_data['var' + str(i).zfill(3)].isnull() == True]
    miss_index = miss_tmp_x.index.values
    miss_x = [t.value // 10 ** 9 - row_x[0] for t in miss_tmp_x['ts']]
    miss_x=miss_x[1000:1100]
    x = [i - row_x[0] for i in row_x]
    # max
    x=[i for i in x if (i>(np.min(miss_x)-100000) and i<(np.max(miss_x)+100000))]
    y = row_data.loc[:, 'var' + str(i).zfill(3)].values
    index = [int((np.max(x) / 2) * (math.cos(((2 * i - 1) * math.pi) / (2 * 30)) + 1)) for i in range(1, 31)]
    j = 0
    index = sorted(index)
    re_index = []
    for i in range(0, len(index)):
        if j >= len(x):
            break
        while x[j] < index[i]:
            j = j + 1
        if (x[j] >= index[i]):
            re_index.append(x[j])
            index[i] = j
            j = j + 1
    y = [y[i] for i in index]
    return re_index, y, miss_x, miss_data, miss_index

def liner_insert():
    row_data = pd.read_csv('all_data.csv', parse_dates=[0])







if __name__ == '__main__':
    #load_data_to_get_xy(1)
    save_interpolate_data()
    #get_data(1,'outer')


