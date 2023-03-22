# -*- coding:utf-8 -*-
"""
Software:PyCharm
File: subway.py
Institution: --- CSU&BUCEA ---, China
E-mail: obitowen@csu.edu.cn
Author：Yanjie Wen
Date：2023年03月16日
My zoom: https://github.com/YanJieWen
"""

import numpy as np
import pandas as pd
import os
import glob
from collections import OrderedDict
from datetime import datetime
from datetime import timedelta
import time

class Data_subway():
    def __init__(self,sub_path,extention,zone_path,gis_path_2016,start,end,freq,max_count,logger):
        self.start = pd.to_datetime(start)-timedelta(hours=freq)#将条件放宽应对部分站点起始的时间不一致
        self.end = pd.to_datetime(end)+timedelta(hours=freq)
        self.freq = freq
        self.max_counter = max_count
        self._logger = logger

        file_list = self.get_files(sub_path, extention)[:-1]
        for index, file in enumerate(file_list):
            if index == 0:
                self.df = pd.read_csv(file, sep=',')
            else:
                self.df = pd.concat([self.df, pd.read_csv(file, sep=',')], axis=0)

        # 去除空格,1.去除空格并拼接站点与线路。2.匹配station表和gis表.3.选取目标时间段内
        self.zones = pd.read_csv(zone_path)
        self.zones_se = self.zones[self.zones['borough'] == 'Manhattan']
        self.z_gis_2016 = pd.read_excel(gis_path_2016)
        # step1
        self.zones_se['station_lines'] = self.zones_se.apply(
            lambda row: self.concat_space(row['station'], row['line_names']), axis=1)
        self.z_gis_2016['station_lines'] = self.z_gis_2016.apply(
            lambda row: self.concat_space(row['stop_name'], row['lines']).upper(),
            axis=1)
        self.df['station_lines'] = self.df.apply(lambda row: self.concat_space(row['STATION'], row['LINENAME']), axis=1)
        # step2
        inte_df = pd.merge(self.zones_se, self.z_gis_2016, on=['station_lines'], how='inner')
        station_info = inte_df[['station', 'line_names', 'station_lines', 'stop_id']]
        # 筛选出曼哈顿区的表,根据station_lines字段进行匹配，因为如果按照station来可能出来的节点在gis中找不到
        station_info.sort_values(by=['stop_id'], ascending=True, inplace=True)
        station_info.drop_duplicates(['station_lines'], keep='first', inplace=True)
        self.df_inter = pd.merge(self.df, station_info, on=['station_lines'], how='inner')  # 根据站-线筛选仅包含的记录表

    @staticmethod
    def get_files(file_path, extention):
        file_glob = os.path.join(file_path + '/' + '*.' + extention)
        file_list = [file.replace('\\', '/') for file in glob.glob(file_glob)]
        return file_list

    @staticmethod
    def concat_space(s, l):
        '''
        拼接站点-线路
        :param s:站点名称
        :param l:线路名称
        :return:站点_线路
        '''
        return s.replace(' ', '') + '_' + l.replace(' ', '')

    @staticmethod
    def get_daily_counts(row, str, max_counter):
        counter = row[str] - row["PREV_{}".format(str)]
        if counter < 0:
            counter = -counter  # 负数进行反转
        if counter > max_counter:  # 数值不能过大
            # print(row[str], row["PREV_{}".format(str)])
            return 0
        return counter

    @staticmethod
    def to_hour(row, str1):
        '''
        时间向下取整
        :param row: 一行
        :param str1: 列字段
        :return:
        '''
        t = row[str1]
        return pd.to_datetime(datetime(t.year, t.month, t.day, t.hour))

    @staticmethod
    def diff_hour(a, b):
        '''

        :param a: 后一个时间
        :param b: 前一个时间
        :return: 前后两个时间相隔小时数
        '''
        return (a - b) / pd.Timedelta(1, 'H')

    def combine_time(self):
        '''
        第1步：合并日期和时间
        :return: 返回一个新的字段表
        '''
        self.df_inter['datetime'] = self.df_inter['DATE'] + ' ' + self.df_inter['TIME']
        self.df_inter['datetime'] = pd.to_datetime(self.df_inter['datetime'])  # 格式化时间
        self.df_inter.columns = [column.strip() for column in self.df_inter.columns]  # 去掉列名的周围空白区域

    def del_dup(self):
        '''
        第2步: 删除重复的数据行
        :return: 删除重复的数据，desc包含了recover数据导致了数据的重复
        '''
        self.df_inter.sort_values(["C/A", "UNIT", "SCP", "STATION", "datetime"], inplace=True, ascending=False)  # 降序排列
        self.df_inter.drop_duplicates(subset=["C/A", "UNIT", "SCP", "STATION", "datetime"], inplace=True)

    def diff_demand(self):
        '''
        第3步：前后时刻需求数据
        由于原始数据存储的是累计的进出站量，因此需要前后时刻的数据量求差才是当前时刻的进出量；并对不合理的数据进行处理：例如后一时刻累计小于前一时刻或者
        前后累计数据差值极大
        :return:
        '''
        df_ = self.df_inter.loc[:, ['C/A', 'UNIT', 'SCP', 'STATION', 'datetime', 'stop_id', 'station_lines', 'ENTRIES', 'EXITS']]
        df_h = df_.groupby(['C/A', 'UNIT', 'SCP', 'STATION', 'station_lines', 'stop_id', 'datetime','EXITS']).ENTRIES.first().reset_index()
        # 做平移当前时间段要减去前一个时间段
        df_h[["PREV_DATE", "PREV_ENTRIES", "PREV_EXITS"]] = (df_h.groupby(['C/A', 'UNIT', 'SCP', 'STATION'
                                                                              , 'station_lines', 'stop_id'])["datetime", "ENTRIES", 'EXITS']
            .transform(lambda grp: grp.shift(1)))  # 入口&出口
        # 第一个数据点为nan需要删除
        df_h.dropna(subset=["PREV_DATE"], axis=0, inplace=True)
        # 选取时间间隔内
        time_split = pd.date_range(start=self.start, end=self.end, freq='{}H'.format(self.freq))  # 增加两个时间节点便于补齐
        time_tuple = [(time, time_split[i + 1]) for i, time in enumerate(time_split) if i != len(time_split) - 1]
        df_se = df_h[(df_h['PREV_DATE'] > self.start) & ((df_h['datetime'] < self.end))]  # 筛选时间段
        df_se["DAILY_ENTRIES"] = df_se.apply(self.get_daily_counts, axis=1, str='ENTRIES', max_counter=self.max_counter)
        df_se["DAILY_EXITS"] = df_h.apply(self.get_daily_counts, axis=1, str='EXITS', max_counter=self.max_counter)
        df_se['datetime'] = df_se.apply(self.to_hour, axis=1, str1='datetime')
        df_se['PREV_DATE'] = df_se.apply(self.to_hour, axis=1, str1='PREV_DATE')
        self.df_inter = df_se
        self.df_inter.sort_values('stop_id', inplace=True)
        return time_tuple

    def uneq_sample(self,time_tuple):
        '''
        第4步:最核心也是最难的步骤，地铁的数据最小的采样时间间隔为4小时。操蛋的是对于部分站点的部分时间段，它可能是1小时，6小时或者非整值，因此我们对这些
        不满足最小采样间隔的时间节点要进行出律
        :param time_tuple: 时间列表
        :return:[n,t,2],[小区索引从小到达排序的列表]
        '''
        stp = self.df_inter.groupby(by='stop_id')  # 以站点的ID为唯一标识，空间维度
        st_dict = OrderedDict()  # 用于存储时空数据
        for id, st_ in stp:
            gp = list(st_.groupby(by='datetime'))  # 时间维度,当前时间可能存在多个出入口数据需要求和,包裹时间的列表
            gp = [(g[0], (g[1]['DAILY_ENTRIES'].sum(), g[1]['DAILY_EXITS'].sum())) for g in gp]  # 先转为站点时间段的需求量
            gp_dict = OrderedDict()  # 存储每一个站点
            if len(gp) < len(time_tuple) - 4 * 6:  # 缺失的数据大于一天则进行过滤
                continue
            # 处理不合理的节点
            else:
                time_ = [g[0] for g in gp]
                diff_ = list(map(self.diff_hour, time_[1:], time_[:-1]))#求前后时间的间隔
                k_ = np.array(diff_) / self.freq
                # 前后间隔大于4小时-->数据缺失
                k = {idx: v for idx, v in enumerate(k_) if v > 1}
                for key, value in k.items():
                    if 1 < value < 2:  # 小于1的采用后一位进行替代
                        padd_num = gp[key + 1][1]
                        padd_time = gp[key + 1][0]
                        gp_dict[padd_time] = padd_num  # 如果加的话则后续的值不会被替代
                    elif value >= 2:
                        for num in range(int(value - 1)):  # 可能有除不尽的情况，采用向上取整
                            padd_num = (np.mean(gp[key][1][0] + gp[key + 1][1][0]),
                                        np.mean(gp[key][1][1] + gp[key + 1][1][1]))  # 前后均值填充
                            time_padd = gp[key][0] + timedelta(hours=4 * (num + 1))
                            gp_dict[time_padd] = padd_num
                        gp_dict[gp[key + 1][0]] = gp[key + 1][1]  # 多次填充保留最后的数值
                #前后时刻间隔小于4小时-->切片聚合
                k1 = {idx: v for idx, v in enumerate(k_) if v < 1}
                d = []
                c = 0
                for key, value in k1.items():
                    if key == 0:
                        gp_dict[gp[key][0]] = gp[key][1]
                    c += value
                    d.append(key)
                    if c == 1:
                        gp_dict[gp[d[-1] + 1][0]] = gp[d[-1] + 1][1]
                        d = []
                        c = 0
                #前后为准确4小时
                k2 = {idx: v for idx, v in enumerate(k_) if v == 1}
                for key, value in k2.items():
                    gp_dict[gp[key][0]] = gp[key][1]
                    gp_dict[gp[key + 1][0]] = gp[key + 1][1]
                gp_dict = {i: gp_dict[i] for i in sorted(gp_dict)}
            if len(gp_dict) == len(time_tuple) - 2:
                # assert len(gp_dict)==len(time_tuple),print('Your code is incorrect,please check it!')
                st_dict[id] = np.reshape(list(gp_dict.values()), (len(time_tuple) - 2, 2))
        datas = np.array([v for k, v in st_dict.items()])  # 从小到大排序
        zone_idx = [k for k, v in st_dict.items()]
        return datas,zone_idx

    def run_step(self):
        '''
        执行纽约地铁处理4个步骤
        :return: [n,t,2],[小区索引从小到达排序的列表]，花费时间
        '''
        start = time.time()
        self._logger.info('There are total 4 steps for NYC subway!')
        self.combine_time()
        self.del_dup()
        time_tuple = self.diff_demand()
        datas,zones =  self.uneq_sample(time_tuple)
        return datas,zones,(time.time()-start)

