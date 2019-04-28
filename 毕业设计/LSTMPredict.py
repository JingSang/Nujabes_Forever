import numpy as np
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import pandas as pd
from keras import models
from keras import layers
from keras import optimizers
np.set_printoptions(threshold=1000000)
np.seterr(divide='ignore',invalid='ignore')


def load_data(i,j):
    print('read :train_dataset/' + str(i).zfill(3) + '/dataset.csv')
    data = pd.read_csv('train_dataset/' + str(i).zfill(3) + '/dataset.csv',parse_dates=[0])
    print('cur col:var0'+str(j).zfill(2))
    y = data.loc[:, 'var0'+str(j).zfill(2)]
    data=data.drop(columns=['var0'+str(j).zfill(2)])
    #data = data.drop(columns=['var020'])
    x = data.iloc[:,2:]
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3)
    return  x_train,y_train,x_test,y_test

# def load_row_data():
#     data = pd.read_csv('data1.csv',parse_dates=[0])
#     y = data.loc[346440:, 'var004']
#     data=data.drop(columns=['var004'])
#     x = data.iloc[346440:,2:]
#     return  x,y
#
# x_pre,y_pre=load_row_data()
# print(x_pre)



def build_model():
    model=models.Sequential()
    # 80 150 300
    #var034，56  200*2  训练210
    #var056  150*3      训练210
    #ar015   80*2       训练210
    #普通 80   训练210
    # model.add(layers.Dense(80,activation='relu',input_dim=67))
    # model.add(layers.Dense(80, activation='relu'))
    # model.add(layers.Dense(80, activation='relu'))
    model.add(layers.Dense(80, activation='relu', input_dim=67))
    #model.add(layers.Dropout(0.65))
    model.add(layers.Dense(1))
    rmsprop=optimizers.RMSprop(lr=0.001, rho=0.9, epsilon=1e-06)
    model.compile(optimizer=rmsprop,loss='mse',metrics=['mae'])
    return model


def normalize(X_all, X_test):

    X_train_test = np.concatenate((X_all, X_test))
    mu = (sum(X_train_test) / X_train_test.shape[0])
    sigma = np.std(X_train_test, axis=0)

    X_train_test_normed = (X_train_test - mu) / sigma

    # Split to train, test again
    X_all = X_train_test_normed[0:X_all.shape[0]]
    X_test = X_train_test_normed[X_all.shape[0]:]
    return X_all, X_test

def train_model(i,j):
    train_data, train_targets, test_data, test_targets = load_data(i,j)
    # 数据归一化
    # 获取每列求平均值
    train_data,test_data=normalize(train_data,test_data)
    x=[]
    train=[]
    test=[]
    model = build_model()
    c=0
    for k in range(1,7):
        print(k)
        model.fit(train_data, train_targets, epochs = k*10, batch_size = 1000,verbose=0)
        train_mse_score,train_mae_score=model.evaluate(train_data,train_targets,verbose=0)
        print('train_mae:',train_mae_score)
        print('train_mse:',train_mse_score)
        test_mse_score, test_mae_score = model.evaluate(test_data, test_targets,verbose=0)
        print('test_mae:',test_mae_score)
        print('test_mse:',test_mse_score)
        c=c+k*10
        x.append(c)
        train.append(train_mse_score)
        test.append(test_mse_score)
    plt.title("newton_interpolation")
    plt.plot(x,train, linestyle='-', marker='o', color='b',label="train values")#蓝点表示原来的值
    plt.plot(x,test,'r', marker='o',label='test values')#插值曲线
    plt.xlabel('x')
    plt.ylabel('y')
    plt.legend(loc=4)#指定legend的位置右下角
    plt.show()

    print('save model:model/'+str(i).zfill(3)+'/var0'+str(j).zfill(2)+'.h5')
    model.save('model/'+str(i).zfill(3)+'/var0'+str(j).zfill(2)+'.h5')
if __name__ == '__main__':
    # 34  38
    train_model(1,15)


