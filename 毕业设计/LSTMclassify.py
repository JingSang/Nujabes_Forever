
import numpy as np
from sklearn.model_selection import train_test_split
import pandas as pd
from keras import models
from keras import layers
from keras.utils import np_utils
import matplotlib.pyplot as plt

np.set_printoptions(threshold=1000000)
def load_data(i,j):
    print('read :train_dataset/' + str(i).zfill(3) + '/dataset.csv')
    data = pd.read_csv('train_dataset/' + str(i).zfill(3) + '/dataset.csv',parse_dates=[0])
    print('cur col:var0' + str(j).zfill(2))
    y = data.loc[:, 'var0' + str(j).zfill(2)]
    data=data.drop(columns=['var0' + str(j).zfill(2)])
    x = data.iloc[:,2:]
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.25)
    y_train=np_utils.to_categorical(y_train,1000,'int')
    y_test=np_utils.to_categorical(y_test,1000,'int')
    return  x_train,y_train,x_test,y_test



def build_model():
    model=models.Sequential()
    model.add(layers.Dense(80,activation='relu',input_dim=67))
    model.add(layers.Dense(80, activation='relu'))
    model.add(layers.Dense(80, activation='relu'))
    model.add(layers.Dense(1000,activation='softmax'))
    model.compile(optimizer='rmsprop',loss='categorical_crossentropy',metrics=['accuracy'])
    return model
#var004 280

def train_model(i,j):
    train_data, train_targets, test_data, test_targets = load_data(i,j)
    # 数据归一化
    # 获取每列求平均值
    mean = train_data.mean(axis=0)
    train_data -= mean
    # 获取每列方差
    std = train_data.std(axis=0)
    train_data /= std

    mean = test_data.mean(axis=0)
    test_data -= mean
    std = test_data.std(axis=0)
    test_data /= std
    model = build_model()
    #var016     var020    var047   var053  var066
    for k in range(1,7):
        print(k)
        model.fit(train_data, train_targets, epochs = 1, batch_size = 1000,verbose=0)
        train_mse_score,train_mae_score=model.evaluate(train_data,train_targets,verbose=0)
        print('train_acc:',train_mae_score)
        #print('train_mse:',train_mse_score)
        test_mse_score, test_mae_score = model.evaluate(test_data, test_targets,verbose=0)
        print('test_acc:',test_mae_score)
        #print('test_mse:',test_mse_score)
        train_pre=model.predict(test_data).astype('int')
        # print(np.argwhere(train_pre == 1)[:, 1])
    print('save model:model/' + str(i).zfill(3) + '/var0' + str(j).zfill(2) + '.h5')
    model.save('model/' + str(i).zfill(3) + '/var0' + str(j).zfill(2) + '.h5')


if __name__ == '__main__':
    train_model(1,16)
