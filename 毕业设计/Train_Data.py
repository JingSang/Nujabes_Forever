import DataUtils
import LSTMPredict,LSTMclassify
import pandas as pd
from tqdm import *

def save_all_data():
    for i in range(1,34):
        DataUtils.save_full_data(i)

#save_all_data()
#15 31 34 42 43  56 58 61 欠拟合,15,31,34,42,43,56,58,61
# 49    58      过拟合
#16,20,47,53,66 分类
def train_each_model(start,end):
    for i in range(start,end):
        print('i:',i)
        for j in range(68,69):
            if j  in (16,20,47,53,66):
                print('classify:', j)
                #LSTMclassify.train_model(i, j)
            else:
                print('predict:', j)
                LSTMPredict.train_model(i, j)






def save_insert_data():
    for i in tqdm(range(1, 34)):
        df = pd.DataFrame()
        data = DataUtils.get_data(i)
        data = data.interpolate(method='nearest', axis=0)
        df = pd.concat([df, data], axis=0)
        sub = df[df.flag == 1].copy().reset_index(drop=True)
        del sub['flag']
        sub["var016"] = sub["var016"].astype(int)
        sub["var020"] = sub["var020"].astype(int)
        sub["var047"] = sub["var047"].astype(int)
        sub["var053"] = sub["var053"].astype(int)
        sub["var066"] = sub["var066"].astype(int)
        res = pd.read_csv('template_submit_result.csv', parse_dates=[0])[['ts', 'wtid']]
        DF = res.merge(sub, on=['wtid', 'ts'], how='inner')
        print('save:result/'+ str(i).zfill(3) + '/miss_data.csv')
        DF.to_csv('result/'+ str(i).zfill(3) + '/miss_data.csv', mode='a', index=False)

if __name__ == '__main__':
    train_each_model(1, 2)