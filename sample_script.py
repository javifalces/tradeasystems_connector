import user_settings
from tradeasystems_connector.manager_trader import ManagerTrader
# %% 2
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.bar_type import BarType
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.util.instrument_util import getInstrumentList
import datetime

manager = ManagerTrader(user_settings=user_settings)
symbols = ['AAPL', 'MSFT', 'SPY']
instrumentList = getInstrumentList(symbolList=symbols, currency=Currency.usd,
                                   asset_type=AssetType.etf)
fromDate = datetime.datetime(year=2013, day=18, month=7)
toDate = datetime.date.today()  # datetime.datetime(year=2018, day=20, month=11)

# %% 3
# %a
dataframe = manager.getHistoricalData(instrument=instrumentList[0], period=Period.day, number_of_periods=1,
                                      fromDate=fromDate, toDate=toDate, bar_type=BarType.time_bar)

# %%b
dataDict = manager.getDataDictOfMatrix(instrumentList=instrumentList, ratioList=[], fromDate=fromDate,
                                       toDate=toDate)
