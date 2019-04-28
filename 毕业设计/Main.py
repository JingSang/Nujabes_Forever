import insert
import Predict_Data
import DataUtils
import Train_Data


if __name__ == '__main__':
    #筛选出缺失的数据----------------运行一次就可以
    #DataUtils.save_all_miss_data_to_directer()

    #筛选出完整的数据----------------运行一次就可以
    #Train_Data.save_all_data()

    #利用完整的数据集作为训练数据，使用神经网络算法训练出预测模型--------------运行一次即可
    Train_Data.train_each_model(1,2)

    # #插值算法运行
    # insert.interpolate()
    #
    # #用神经网络进行预测
    # Predict_Data.predict_data()
    #
    # #将预测值填补回缺失的数据集中








