# NYC-Manhattan-Traffic
This is a data processing project that involves the three-peak multimodal transportation system in New York City's Manhattan borough, including information on yellow taxis, subways, and shared bicycles. Additionally, it also encompasses external attribute data such as Points of Interest (POI) and weather in New York.


## Contents

- [Data Description](#Data-Description)
- [Instructions](#Instructions)
	- [Yellow Taxi](#Yellow-Taxi)
	- [Subway](#Subway)
  	- [Bike](#Bike)
  	- [Secondary Information](#SecondaryInformatio)
- [How to load](#How-to-load)
- [Contributing](#contributing)
- [License](#license)


## Data-Description
The temporal range of the data is from `0:00 on March 1st, 2018` to `0:00 on September 1st, 2018`. Due to the sampling interval of the subway data being `4 hours`, the unified time interval is also set to `4 hours`, resulting in a total of `1,104` data nodes for half a year. Additionally, this project is applicable to other time periods. The download link for the source data can be found below. This project provides a three-peak system for Manhattan, New York, including [Yellow Taxis](datasets/yellow_taxi), [Subways](datasets/subway), and [Citi Bike](datasets/bike). Furthermore, it also includes additional external attributes such as [POI and Weather data](datasets/external).


## Instructions
The [data_main.py](data_main.py) is the entry point of the program, if you want to execute this program please run 
```
python data_main.py
```

in the terminal.
Next, we will introduce the details of each mode of transportation separately.

### Yellow-Taxi
纽约黄牌出租车的数据获取地址：[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).  
数据的扩展名为：```PARQUET```   
黄牌出租和绿牌出租的区别： 黄色出租(Yellow TAXI)车可以在纽约五大区（布朗克斯区、布鲁克林区、曼哈顿、皇后区、斯塔滕岛）内任何地点搭载乘客。绿色出租车(Green TAXI)则被规定只允许在上曼哈顿、布朗克斯区、皇后区和斯塔滕岛接客.  
数据处理的流程：

