# -*- coding:utf-8 -*-
"""
Software:PyCharm
File: bike.py
Institution: --- CSU&BUCEA ---, China
E-mail: obitowen@csu.edu.cn
Author：Yanjie Wen
Date：2023年03月21日
My zoom: https://github.com/YanJieWen
"""

import zipfile
import numpy as np
import pandas as pd
import os
import glob
import time
import collections
import scipy.sparse as sp
from datetime import datetime

class Data_bike():
    def __init__(self,logger,file_path,extention,start,end,zone_path,check_out_the,check_in_the,max_trip_time,min_trip_time,freq):
        file_glob = os.path.join(file_path + '/' + '*.' + extention)
        file_list = [file.replace('\\', '/') for file in glob.glob(file_glob)]
        for idx, file in enumerate(file_list):
            f = zipfile.ZipFile(file, 'r')
            for file in f.namelist():
                file_ = os.path.join(file_path, file)
                if idx == 0:
                    self.df = pd.read_csv(file_)
                else:
                    self.df = pd.concat([self.df, pd.read_csv(file_)], axis=0)
                # f.extract(file,file_path)
            f.close()
            self.time_split = pd.date_range(start=start, end=end, freq='{}H'.format(freq))
            self.time_tuple = [(time, self.time_split[i + 1]) for i, time in enumerate(self.time_split) if i != len(self.time_split) - 1]
            self.zones =  pd.read_csv(zone_path,sep=',')
            self._logger = logger
            self.check_out_the = check_out_the
            self.check_in_the = check_in_the
            self.max_trip_time = max_trip_time
            self.min_trip_time = min_trip_time

    @staticmethod
    def transform_date(df,time_split):
        '''
        step1:时间处理，包括将时间字段转为日期属性，筛选关注时间范围内的晒寻
        :param df: 数据框
        :param time_split: 时间切片
        :return: 时间筛选后的数据框
        '''
        df.drop(['bikeid', 'usertype', 'birth year', 'gender'], axis=1, inplace=True)
        df['starttime'] = df['starttime'].apply(lambda x: datetime.strptime(x.split('.')[0], '%Y-%m-%d %H:%M:%S')) #waste time
        df['stoptime'] = df['stoptime'].apply(lambda x: datetime.strptime(x.split('.')[0], '%Y-%m-%d %H:%M:%S'))
        df_t_se = df[(df['starttime'] >= time_split[0]) & (df['stoptime'] <= time_split[-1])]
        df_t_se.dropna(axis=0, how='any', inplace=True)

    @staticmethod
    def select_mahat(df_t_se,zones):
        '''
        step2: 关注曼哈顿区
        :param df_t_se: 数据框
        :param zones: 曼哈顿区的数据框
        :return:仅包含曼哈顿的共享单车站点
        '''
        zone_id = list(set(zones['id'].values))
        return df_t_se[(df_t_se['start station id'].isin(zone_id)) & (df_t_se['end station id'].isin(zone_id))]



    @staticmethod
    def drop_low_demand(df_, str, max):
        check_out = df_.groupby(by=str)
        drop_id = []
        for id, df in check_out:
            count = df['starttime'].count()
            if count < max:
                drop_id.append(id)
        df_.drop(df_[df_[str].isin(drop_id)].index, inplace=True)

    def filter_low_demand(self,df):
        '''
        step3:过滤掉低签入和低签出的站点
        :param df_: 数据框
        :param check_out_the: 签出阈值
        :param check_in_the: 签入阈值
        :return: 数据框
        '''
        self.drop_low_demand(df, 'start station id', self.check_out_the)  # 过滤低签出
        self.drop_low_demand(df, 'end station id', self.check_in_the)  # 过滤低签入

    @staticmethod
    def filter_tripduration(df_,max_trip_time,min_trip_time):
        '''
        step4:过滤掉异常的行程记录
        :param df_: 数据框
        :param max_trip_time: 最大出行时间
        :param min_trip_time: 最小出行时间
        :return: 筛选后的数据框
        '''
        df_.drop(df_[(df_['tripduration'] > max_trip_time) | (df_['tripduration'] < min_trip_time)].index, inplace=True)

    @staticmethod
    def to_od(df_,time_tuple):
        '''
        step5:将数据保存为oddict
        :param df_:数据框
        :param time_tuple: 时间列表
        :return: 字典+从小到达排序的小区索引
        '''
        df_.sort_values(by='start station id', inplace=True)
        s_id = list(set(df_['start station id'].values.tolist()))
        e_id = list(set(df_['end station id'].values.tolist()))
        all_id = list(set(s_id + e_id))
        zone_dict = {zi: id for id, zi in enumerate(sorted(all_id))}
        df_['new_s_id'] = list(map(zone_dict.get, df_['start station id'].values))
        df_['new_e_id'] = list(map(zone_dict.get, df_['end station id'].values))
        od_dict = collections.OrderedDict()
        for idx, time_ in enumerate(time_tuple):
            con_ = df_[((df_['starttime'] >= time_[0]) & (df_['starttime'] < time_[1])) | (
                    (df_['stoptime'] >= time_[0]) & (df_['stoptime'] < time_[1]))]
            log_ = con_.iloc[:, -2:].values
            od_matrix = np.zeros((len(all_id), len(all_id)))
            for od in log_:
                od_matrix[od[0], od[1]] += 1
            od_matrix = sp.csr_matrix(od_matrix, dtype=np.float32)
            od_dict[idx] = [od_matrix, time_]
            return od_dict,list(zone_dict.keys())

    def run_process(self):
        self._logger.info('--Begining!There are total 5 steps for NYC bike!')
        s = time.time()
        self.transform_date(self.df,self.time_split)
        df_t_se=self.select_mahat(self.df,self.zones)
        self.filter_low_demand(df_t_se)
        self.filter_tripduration(df_t_se,self.max_trip_time,self.min_trip_time)
        dict_,zone_id = self.to_od(df_t_se,self.time_tuple)
        return dict_,zone_id,(time.time()-s)

