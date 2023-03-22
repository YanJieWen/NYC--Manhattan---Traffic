# NYC-Manhattan-Traffic
This is a data processing project that involves the three-peak multimodal transportation system in New York City's Manhattan borough, including information on yellow taxis, subways, and shared bicycles. Additionally, it also encompasses external attribute data such as Points of Interest (POI) and weather in New York.


## Contents

- [Data Description](#Data-Description)
- [Instructions](#Instructions)
	- [Yellow Taxi](#Yellow-Taxi)
	- [Subway](#Subway)
  	- [Bike](#Bike)
  	- [Secondary Information](#SecondaryInformation)
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
**Processing**: orders with unreasonable travel time and travel distance are dropped.    
**Description**: After deleting the partitions of the islands, there are a total of ``63`` partitions in Manhattan, and the adjacency matrix is numbered from small to large. A total of ``29.25 million`` travel records, with an average daily travel volume of ``15.9W``.   
**Output Format**: Data is persisted as `pkl`.Format is`Orderdict{0:[Sparse OD,TIMESTEMP]}`    
**More details**: [ytaxi.py](datasets/yellow_taxi/ytaxi.py)    
**How to run it**: ``` python data_main.py --type 0```    


### Subway
**Data source address**：[MTA](https://toddwschneider.com/dashboards/nyc-subway-turnstiles/).  
**Extension**：```txt```   
**Station Match**:  The spatial data of subway stations has troubled me for a long time. There is a difference between the geographic file [MTA NYC Transit Subway](https://archive.nyu.edu/handle/2451/34759) and the one provided by the [MTA](https://github.com/toddwschneider/nyc-subway-turnstile-data/blob/master/lib/stations.csv). We stitch it manually according to the station-line.   
**Processing**: The core task is the processing of the temporal dimension, which includes `4` parts: delete duplicate data; delete records where the cumulative data of the next day is less than the cumulative data of the previous day; Delete the value with too much input and output (set to `0`); since some stations are not strictly 4-hour sampling, we time-align them.  
**Description**: A total of `93` stations were adopted after data processing.   
**Output Format**: Data is persisted as `pkl`.Format is`[[N,T,2],STATION INDEX]`  
**More details**: [subway.py](datasets/subway/subway.py)  
**How to run it**: ```python data_main.py --file_path ./datasets/subway --type 2 --extenstion txt --zone_path  ./datasets/subway/stations.csv --pkl_path subway.pkl```


### Bike
**Data source address**：[Citi Bike](https://citibikenyc.com/system-data).  
**Extension**：```ZIP```   
**Station Match**:  After GIS screening, Manhattan contains a total of `439` stations.   
**Processing**:   1. Delete `nan` data. 2. Delete orders with abnormal trip duration. 3. Delete sites with too few check-in and check-out.   
**Description**: A total of `427` stations were adopted after data processing. The total travel records are `725w`, with an average of `3.9w` per day  
**Output Format**: Data is persisted as `pkl`.Format is`[Orderdict[0:[Sparse OD,TIMESTEMP]],[zones index]]`  
**More details**: [bike.py](datasets/bike/bike.py)  
**How to run it**: ```python data_main.py --file_path ./datasets/bike --type 1 --extenstion zip --zone_path  ./datasets/bike/station_id_mat.csv --pkl_path bike.pkl```  
**NOTE**: **The demand for citi bike is extremely sparse. In order to improve the availability of data, most methods are to cluster the shared bicycle station first. You can refer to this [paper](https://dl.acm.org/doi/abs/10.1145/2820783.2820837)**  



### SecondaryInformation
**Data source address**：[Synoptic Data](https://developers.synopticdata.com/)&[POI](https://data.cityofnewyork.us/City-Government/Points-Of-Interest/rxuy-2muj).  
**Extension**：```CSV&ZIP```     
**Processing**: Meteorological data fills forward the weather conditions and wind speed data. According to the [literature](https://dl.acm.org/doi/abs/10.1145/2820783.2820837)  , the weather conditions are divided into 4 categories, `snowy`, `rainy`, `foggy`, and `sunny`. The POI data counts the number of each class in each Manhattan subdivision according to `13` types.  
**Description**: Weather condition: If there is snow and rain in the time interval, it is considered snow; if it is rainy and foggy, it is considered foggy; if it is foggy and sunny, it is considered foggy.  
**Output Format**: Data is persisted as `pkl`.Format is`Orderdict[[Average temperature, average relative humidity, average wind speed, average visibility, average weather conditions],[Timestemp]]`   
**How to run it**: I'm sure you can write a beautiful piece of logical code to handle them. However, POI may be accessed by `ArcGis`.



## How-to-load







