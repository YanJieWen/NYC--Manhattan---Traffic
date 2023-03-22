# -*- coding:utf-8 -*-
"""
Software:PyCharm
File: d2p.py
Institution: --- CSU&BUCEA ---, China
E-mail: obitowen@csu.edu.cn
Author：Yanjie Wen
Date：2023年03月12日
My zoom: https://github.com/YanJieWen
"""
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import os
import glob
import time
import collections
import scipy.sparse as sp


class Data_taxi():
    def __init__(self,logger,file_path,extenstion,use_cols,new_cols,zone_path,lower_limit_sec,upper_limit_sec,
                 lower_limits_trip_dis,upper_limits_trip_dis,freq,strat,end):
        self._logger = logger
        self.file_path = file_path
        self.extention = extenstion
        self.use_cols = use_cols
        self.new_cols = new_cols
        self.file_list = self.get_files(file_path,extenstion)
        self.lower_limit_sec = lower_limit_sec
        self.upper_limit_sec = upper_limit_sec
        self.lower_limits_trip_dis = lower_limits_trip_dis
        self.upper_limits_trip_dis = upper_limits_trip_dis
        self.freq = freq
        self.start = strat
        self.end = end
        zones = pd.read_excel(zone_path)
        self.loc_id = zones['LocationID'].values
        self._logger.info('{} zones in total at Manhatten'.format(len(self.loc_id)))
        st = time.time()
        for index, file in enumerate(self.file_list):
            if index == 0:
                df = self.to_pd(file)
                df = self.deal_cols(df, self.use_cols, self.new_cols,self.loc_id)
            else:
                df = pd.concat([df, (self.deal_cols(self.to_pd(file), use_cols, new_cols,self.loc_id))], axis=0)
        self.trips = df
        self._logger.debug('There are {} files in NYC, taking {:.1f} Sec!'.format(len(self.file_list),time.time()-st))

    def filter_data(self,data):
        '''

        :param data:过滤后仅包含曼哈顿区的出租出行记录
        :return:根据距离和出行时间上下限筛选后的出行数据
        '''
        se_data = data[(data[self.new_cols[0]] > self.lower_limit_sec) & (data[self.new_cols[0]] < self.upper_limit_sec) &
                       (data[self.use_cols[-1]] > self.lower_limits_trip_dis) & (
                    data[self.use_cols[-1]] < self.upper_limits_trip_dis)]  # 去除负数和
        se_data.set_index(np.arange(se_data.shape[0]), inplace=True)
        return se_data

    def gen_data_dict(self):
        '''
        将处理后的曼哈顿数据存储为顺序字典
        :return: {idx:[稀疏矩阵，时间戳]}
        '''
        se_data = self.filter_data(self.trips)
        sort_zones = sorted(list(set(se_data[self.use_cols[2]].values.tolist() + se_data[self.use_cols[3]].values.tolist())))
        id_map = {value: id for id, value in enumerate(sort_zones)}
        time_split = pd.date_range(start=self.start, end=self.end, freq='{}H'.format(self.freq))
        time_tuple = [(time, time_split[i + 1]) for i, time in enumerate(time_split) if i != len(time_split) - 1]
        pick_drop_zones = se_data.loc[:, [self.use_cols[2], self.use_cols[3]]].values
        new_od_zones = np.asarray(list(map(id_map.get, pick_drop_zones.flatten())), dtype=np.int32).reshape(
            pick_drop_zones.shape)
        se_data.loc[:, ['pu_zones', 'do_zones']] = new_od_zones
        o_dict = collections.OrderedDict()
        for idx, time_ in enumerate(time_tuple):
            con_ = se_data[((se_data[self.use_cols[0]] >= time_[0]) & (se_data[self.use_cols[0]] < time_[1])) | (
                        (se_data[self.use_cols[1]] >= time_[0]) & (se_data[self.use_cols[1]] < time_[1]))]
            con_.sort_values(self.use_cols[1], inplace=True)
            log_ = con_.iloc[:, -2:].values
            od_matrix = np.zeros((len(self.loc_id), len(self.loc_id)))
            for od in log_:
                od_matrix[od[0],od[1]] += 1
            od_matrix = sp.csr_matrix(od_matrix, dtype=np.float32)
            o_dict[idx] = [od_matrix, time_]
        return o_dict

    @staticmethod
    def get_files(file_path, extention):
        '''

        :param file_path: 数据存放目录
        :param extention: 数据文件的后缀
        :return: 文件夹下所有的数据文件
        '''
        file_glob = os.path.join(file_path + '/' + '*.' + extention)
        file_list = [file.replace('\\', '/') for file in glob.glob(file_glob)]
        return file_list
    @staticmethod
    def to_pd(file):
        '''

        :param file:每个月的数据文件
        :return:数据框
        '''
        return pq.read_table(file).to_pandas()
    @staticmethod
    def deal_cols(data, use_cols, new_cols,loc_id):
        '''

        :param data:总的数据框
        :param use_cols:需要用到的既有字段
        :param new_cols:记录每一条出行所花费的时间
        :param loc_id:筛选仅包含曼哈顿区的分区
        :return:
        '''
        data = data.loc[:, use_cols]
        data[new_cols[0]] = ((data[use_cols[1]] - data[use_cols[0]]) / pd.Timedelta(1, 'S')).fillna(0).astype(int)
        return data[(data[use_cols[2]].isin(loc_id))&(data[use_cols[3]].isin(loc_id))]

