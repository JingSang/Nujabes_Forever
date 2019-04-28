import matplotlib.pyplot as plt
from DataUtils import load_data_to_get_xy,load_data_to_get_xy1
import numpy as np
import pandas as pd
import math


def get_diff_table(X, Y):
    """
    得到插商表
    动态规划
    i,j表示f[xi.......xj]的值
    """
    n = len(X)
    A = np.zeros([n, n])
    for i in range(0, n):
        A[i][0] = Y[i]
    for i in range(1,n):
        for j in range(0,n-i):
            A[j,i]=(A[j+1,i-1]-A[j,i-1])/(X[i+j]-X[j])
    return A

def newton_interpolation(X,Y,x,A):
    """
    计算x点的插值
    """
    sum=Y[0]
    for i in range(1,len(X)):
        tmp = A[0,i]
        for j in range(0,i):
            tmp*=(x-X[j])
        sum+=tmp
    return sum


"""
@brief:在训练数据中选取点来生成多项式插值，对缺失值进行插值。根据缺失值的Inedx将插值后数据填补回miss_data中。
@process:
"""
def interpolate():
    concat_df = pd.DataFrame()
    row_data = pd.read_csv('train_dataset/001/dataset.csv', parse_dates=[0])
    miss_data = pd.read_csv('miss_dataset/001/miss_data.csv', parse_dates=[0])
    for i in range(1,69):
        print(i)
        X,Y,xs,miss_data,miss_index=load_data_to_get_xy(i,row_data,miss_data)
        A = get_diff_table(X,Y)
        ys=[]
        for x in xs:
            ys.append(newton_interpolation(X,Y,x,A))
        ys=np.round(ys,decimals=2)

        for j in range(0,len(miss_index)):
            if i in (16, 20, 47, 53, 66):
                ys[j]=round(abs(ys[j]))
            miss_data.loc[[miss_index[j]],['var'+str(i).zfill(3)]]=ys[j]
        if i==1:
            concat_df=pd.concat([concat_df,miss_data['ts']],axis=1)
            concat_df = pd.concat([concat_df, miss_data['wtid']], axis=1)
            concat_df = pd.concat([concat_df, miss_data['var001']], axis=1)
        else:
            concat_df = pd.concat([concat_df, miss_data['var'+str(i).zfill(3)]], axis=1)
        plt.title("newton_interpolation")
        plt.plot(X,Y, linestyle='-', marker='o', color='b',label="original values")#蓝点表示原来的值
        plt.plot(xs,ys,'r', marker='o',label='interpolation values')#插值曲线
        plt.xlabel('x')
        plt.ylabel('y')
        plt.legend(loc=4)#指定legend的位置右下角
        plt.show()
    concat_df.to_csv('interpolate_data.csv', index=False)

def interpolate1():
    concat_df = pd.DataFrame()
    row_data = pd.read_csv('train_dataset/001/dataset.csv', parse_dates=[0])
    miss_data = pd.read_csv('miss_dataset/001/miss_data.csv', parse_dates=[0])
    for i in range(1,69):
        print(i)
        X,Y,xs,miss_data,miss_index=load_data_to_get_xy1(i,row_data,miss_data)
        A = get_diff_table(X,Y)
        ys=[]
        for x in xs:
            ys.append(newton_interpolation(X,Y,x,A))
        ys=np.round(ys,decimals=2)
        plt.title("newton_interpolation")
        plt.plot(X,Y, linestyle='-', marker='o', color='b',label="original values")#蓝点表示原来的值
        plt.plot(xs,ys,'r', marker='o',label='interpolation values')#插值曲线
        plt.xlabel('x')
        plt.ylabel('y')
        plt.legend(loc=4)#指定legend的位置右下角
        plt.show()


def plot():
    row_data = pd.read_csv('train_dataset/001/dataset.csv', parse_dates=[0])
    polynomial_data = pd.read_csv('polynomial_data.csv', parse_dates=[0])
    predict_data = pd.read_csv('finalresult2.csv', parse_dates=[0])
    fe = [i for i in row_data.columns if 'var' in i]
    X=row_data['ts']
    x=polynomial_data['ts']
    p_x=predict_data['ts']
    for i in fe:
        print(i)
        Y=row_data[i]
        y=polynomial_data[i]
        p_y=predict_data[i]
        # plt.title("newton_interpolation")
        # plt.plot(X,Y, linestyle='-', marker='o', color='b',label="original values")#蓝点表示原来的值
        # plt.xlabel('x')
        # plt.ylabel('y')
        # plt.legend(loc=4)#指定legend的位置右下角
        # plt.show()
        # 画图显示
        fig = plt.figure(figsize=(10, 8))
        ax1 = fig.add_subplot(211)
        ax1.plot(X,Y,marker='o', color='b',label="original values")#蓝点表示原来的值

        ax2 = fig.add_subplot(212)
        ax2.plot(x,y,marker='o', color='b',label="original values")#蓝点表示原来的值
        # ax3= fig.add_subplot(313)
        # ax3.plot(p_x,p_y, linestyle='-', marker='o', color='b',label="original values")#蓝点表示原来的值
        plt.show()
if __name__ == '__main__':
    plot()