# tradeasystems_connector <br>
## common framework to  connect to different algorithmic trading providers
### Project needs to be developed more/test/ need more work , but save/load data normalizing format of all providers.
#### use other open source libraries: pystore 
The idea is to get a common framework easy to add new providers<br>
Connector to different brokers
get historical data and save into pystore files


### brokers/data:<br>
    *oanda
    *gdax
    *fxcm
    *ib

### only data:<br>
    *cryptocompare
    *quandl
    *alphavantage
    *bloomberg
## Requirements:
	visual studio:: https://visualstudio.microsoft.com/es/vs/community/
	Anaconda 3.6 :: https://www.anaconda.com/download
	Add to path   ::
	    TRADEA_LOG_PATH
	    TRADEA_DATABASE_PATH
	install python - snappy -> requirements or https://www.lfd.uci.edu/~gohlke/pythonlibs/
	pip install requirements.txt
	Numpy version 1.14 and pandas version 0.20.3
## install 
    pip install git+https://github.com/tradeasystems/tradeasystems_connector.git
	
### How to download data/persist (sample_script.py):<br>
    1- Configure user_settings with your credentials and configuration 
    2- manager = ManagerTrader(user_settings=user_settings)
    3a- download pandas dataframe to work sequentially -> dataDict = manager.getDataDictOfMatrix(instrumentList=instrumentList, ratioList=[], fromDate=fromDate,
                                           toDate=toDate)
    3b- download dict to work crosssectional -> dataDict = manager.getDataDictOfMatrix(instrumentList=instrumentList, ratioList=[], fromDate=fromDate,
                                           toDate=toDate)

