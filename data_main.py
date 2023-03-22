# -*- coding:utf-8 -*-
"""
Software:PyCharm
File: data_main.py
Institution: --- CSU&BUCEA ---, China
E-mail: obitowen@csu.edu.cn
Author：Yanjie Wen
Date：2023年03月12日
My zoom: https://github.com/YanJieWen
"""

from utils import fashion_logging
import argparse
import pickle
from datasets.yellow_taxi import ytaxi
from datasets.subway import subway
from datasets.bike import bike

parser = argparse.ArgumentParser('--多模态数据集处理脚本--')
parser.add_argument('--log_path',default='./utils/note.log',dest='log_path',type=str,help='保存运行日志的文件')
parser.add_argument('--clevel',default='INFO',dest='clevel',type=str,help='cmd中运行的显示级别，INFO,ERROR,DEBUG')
parser.add_argument('--flevel',default='INFO',dest='flevel',type=str,help='文件运行显示级别')
parser.add_argument('--type',default=0,dest='type',type=int,help='0-出租车，1-自行车，2-地铁')

parser.add_argument('--file_path',default='./datasets/yellow_taxi',dest='file_path',type=str,help='存储纽约黄牌汽车的目录')
parser.add_argument('--extenstion',default='parquet',dest='extenstion',type=str,help='原始数据后缀')
parser.add_argument('--zone_path',default='./datasets/yellow_taxi/matlc_zone.xlsx'
                    ,dest='zone_path',type=str,help='曼哈顿地区的分区索引')
parser.add_argument('--freq',default=4,dest='freq',type=int,help='时间采样间隔')
parser.add_argument('--pkl_path',default='taxi.pkl',dest='pkl_path',type=str,help='数据持久化文件名')
parser.add_argument('--start_time',default='2018-03-01 00:00:00',dest='start_time',type=str,help='开始时间')
parser.add_argument('--end_time',default='2018-09-01 00:00:00',dest='end_time',type=str,help='结束时间')
#=======================================================================================================================
#纽约黄牌出租车处理
parser.add_argument('--use_cols',default=['tpep_pickup_datetime','tpep_dropoff_datetime','PULocationID','DOLocationID',
                                          'trip_distance'],dest='use_cols',type=list,help='筛选的字段属性')
parser.add_argument('--new_cols',default=['trip_sec'],dest='new_cols',type=list,help='新添的数据列，存储载客的时间')
parser.add_argument('--lower_limit_sec',default=5*60,dest='lower_limit_sec',type=int,help='运行时间阈值下限')
parser.add_argument('--upper_limit_sec',default=60*60*4,dest='upper_limit_sec',type=int,help='运行时间阈值上限')
parser.add_argument('--lower_limits_trip_dis',default=1,dest='lower_limits_trip_dis',type=int,help='运行距离阈值下限')
parser.add_argument('--upper_limits_trip_dis',default=100,dest='upper_limits_trip_dis',type=int,help='运行距离阈值上限')
#=======================================================================================================================
#纽约地铁数据
parser.add_argument('--gis_path_2016',default='./datasets/subway/sub_gis_2016.xlsx',dest='gis_path_2016',type=str,
                    help='由纽约市立大学提供，操蛋的是很多站点-线路对不上，我们手动修改了一些，保留了128个站点')
parser.add_argument('--max_counter',default=100000,dest='max_counter',type=int,help='筛选需求的阈值')
#=======================================================================================================================
#纽约共享单车数据
parser.add_argument('--check_out_the',default=1000,dest='check_out_the',type=int,help='集计起来站点的签出最小阈值')
parser.add_argument('--check_in_the',default=1000,dest='check_in_the',type=int,help='集计起来站点的签入最小阈值')
parser.add_argument('--max_trip_time',default=3600,dest='max_trip_time',type=int,help='最大的出行时间')
parser.add_argument('--min_trip_time',default=60,dest='min_trip_time',type=int,help='最小的出行时间')
#=======================================================================================================================


def to_pickle(pk_path,data):
    file = open(pk_path, 'wb')
    pickle.dump(data, file)
    file.close()
def main():
    args = parser.parse_args()
    type = args.type
    logging = fashion_logging.Logger(args.log_path,args.clevel,args.flevel)

    if type==0:
        logging.info('--Now Loading NYC Yellow Taxi--')
        data = ytaxi.Data_taxi(logging,args.file_path,args.extenstion,args.use_cols,args.new_cols,args.zone_path,
                               args.lower_limit_sec,args.upper_limit_sec,args.lower_limits_trip_dis,
                               args.upper_limits_trip_dis,args.freq,args.start_time,
                                  args.end_time)
        ods = data.gen_data_dict()
        to_pickle(args.file_path+'/'+args.pkl_path,ods)
        logging.info('--Datasest has been stored! There are {} samples in total!--'.format(len(ods)))
    elif type==1:
        logging.info('--Now Loading NYC City Bike--')
        data = bike.Data_bike(logging,args.file_path,args.extenstion,args.start_time,
                                  args.end_time,args.zone_path,args.check_out_the,args.check_in_the,
                              args.max_trip_time,args.min_trip_time,args.freq)
        dict,zone_id,time = data.run_process()
        to_pickle(args.file_path + '/' + args.pkl_path,[dict, zone_id])
        logging.info('--Datasest has been stored! There are {} stations in total, '
                     'and spend {:.1f}s!--'.format(len(zone_id),time))
    elif type==2:
        logging.info('--Now Loading NYC Subway--')
        data = subway.Data_subway(args.file_path,args.extenstion,args.zone_path,args.gis_path_2016,args.start_time,
                                  args.end_time,args.freq,args.max_counter,logging)
        datas, zones, time = data.run_step()
        to_pickle(args.file_path + '/' + args.pkl_path, [datas,zones])
        logging.info('--Datasest has been stored! There are {} stations in total, '
                     'and spend {:.1f}s!--'.format(len(zones),time))

if __name__ == '__main__':
    main()