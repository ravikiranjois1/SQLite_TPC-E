# Evaluating the performance of SQLite DB using TPC-E Benchmark

## authors
* Suhas Chikkanaravangala Vijayakumar
* Ravikiran Jois Yedur Prabhakar
* Karanjit Singh

## pip requirements
* glob
* sqlite3
* pandas
* matplotlib
* numpy

### TPC_E-generation
* Generate the flat out files from TPC-E benchmark available at [http://www.tpc.org/tpce/](http://www.tpc.org/tpce/) 
* after generating the flat out make a directory named 'raw-data' in root folder
* execute ```python3 src/extract.py ``` this will load the csv files into raw-data directory
#### SQLite database
* enter the SQLite3 shell type ```sqlite3``` in terminal
* change the mode to csv type in the sqlite3 shell type 
```.mode=csv```
* create tables
```.read ./scripts/1_create_table.sql```
* load data to the tables ```.read ./scripts/load_data.sql```
* create indexes ```.read ./scripts/4_create_index.sql```
*create foreign key indexes ```.read ./scripts/4_create_fk_index.sql```
#### Execute Trannsactions
* use ```python3 ./src/transactions/trade_<transaction_name>_<transaction_type>.py```
* Transactions diretory has four transactions trade_order,trade_update,trade_lookup and trade_status.
* Transactions directory also has inmemroy version of 4 transactions mentioned , these transactions creates database inmemory and executes frames in a single process.
* note logging should be turned on using ```PRAGMA journal_mode=WAL;```
* All the data(operations performed) will be stored .json files
#### Visualize Performance
* visualize the performance of different database configurations 
 ```
 python3 ./src/plot.py
 ```
