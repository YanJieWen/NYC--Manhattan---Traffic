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
**Data source address**：[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).  
**Extension**：```PARQUET```   
**The difference between Yellow Taxi and Green Taxi**: Yellow taxi (Yellow TAXI) can pick up passengers anywhere in the five major districts of New York (Bronx, Brooklyn, Manhattan, Queens, Staten Island). Green taxis are only allowed to pick up passengers in Upper Manhattan, the Bronx, Queens and Staten Island.
**Processing**: orders with unreasonable travel time and travel distance are dropped。
**Description**: After deleting the partitions of the islands, there are a total of ``63`` partitions in Manhattan, and the adjacency matrix is numbered from small to large. A total of ``29.25 million`` travel records, with an average daily travel volume of ``15.9W``. 
**Output Format**: Data is persisted as `pkl`.Format is`Orderdict{0:[Sparse OD,TIMESTEMP]}`  
**More details**: [ytaxi.py](datasets/yellow_taxi/ytaxi.py)  
**How to run it**: ``` python data_main.py --type 0```  


