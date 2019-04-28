import pandas as pd
from keras.models import load_model
import LSTMPredict

def load_insert_data(i):
    print("loading insert data")
    data = pd.read_csv('polynomial_data.csv', parse_dates=[0])
    print('droping col:',i)
    data = data.drop(columns=i)
    x = data.iloc[:, 2:]
    return x



def predict_data():
    data = pd.read_csv('polynomial_data.csv', parse_dates=[0])
    row_data=pd.read_csv('train_dataset/001/dataset.csv', parse_dates=[0])
    fe = [i for i in data.columns if 'var' in i]
    for j in fe:
        print('cur col:',j)
        model = load_model('model/001/'+j+ '.h5')
        insert_data=load_insert_data(j)
        row_data1 = row_data.drop(columns=j)
        row_data1 = row_data1.iloc[:, 2:]
        #归一化
        print('normalizationing')
        # mean = insert_data.mean(axis=0)
        # insert_data -= mean
        # # 获取每列方差
        # std = insert_data.std(axis=0)
        # insert_data /= std
        insert_data,row_data1=LSTMPredict.normalize(insert_data,row_data1)
        #insert_data.to_csv('insert_data.csv', index=False)
        if j in ('var016', 'var020', 'var047', 'var053', 'var066'):
            print('classifying')
            predict=model.predict(insert_data).astype('int')
            # print(predict.argmax(1))
            data[j]=predict.argmax(1)
        else:
            print('pridecting')
            predict = model.predict(insert_data)
            data[j] = predict
            data[j] = data[j].round(decimals=2)
            print(data[j])
            print('save:finalresult2.csv')
            data.to_csv('finalresult2.csv', index=False)
    print('save:finalresult2.csv')
    data.to_csv('finalresult2.csv',index=False)

def test():
    miss_data = pd.read_csv('polynomial_data.csv', parse_dates=[0])
    fe = [i for i in miss_data.columns if 'var' in i]
    for i in fe:
        miss_tmp_x = miss_data[miss_data[i].isnull() == True]
        print(miss_tmp_x)
        print(i)


if __name__ == '__main__':
    predict_data()







