from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.model.bar_type import BarType
from tradeasystems_connector.service.factor_service import FactorService
from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService
from tradeasystems_connector.service.trading_service import TradingService


class ManagerTrader:
    user_settings = None
    historical_market_data_service = None
    trading_service = None
    factor_service = None

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.historical_market_data_service = HistoricalMarketDataService(self.user_settings)
        self.trading_service = TradingService(self.user_settings)
        self.factor_service = FactorService(self.user_settings)

    def getHistoricalData(self, instrument, period, number_of_periods, fromDate, toDate=None,
                          bar_type=BarType.time_bar):
        # Need to be on pystore
        logger.info("Request historical data from db for %s_%s from %s to %s" % (
            instrument.symbol, instrument.currency, fromDate, toDate))
        return self.historical_market_data_service.getHistoricalData(instrument, period, number_of_periods, fromDate,
                                                                     toDate, bar_type)

    def getFactorData(self, instrumentList, ratioList, fromDate, toDate=None):
        logger.info("Request factor data from db for\ninstrumentList: %s   \nratioList:%s \nfrom %s to %s" % (
            instrumentList, ratioList, fromDate, toDate))
        return self.factor_service.getDataDictOfMatrixAlphalens(instrumentList=instrumentList, ratioList=ratioList,
                                                                fromDate=fromDate, toDate=toDate)

    def getDataDictOfMatrix(self, instrumentList, ratioList, fromDate, toDate=None, persistTempFile=None):
        dictOfMatrix = self.factor_service.getDataDictOfMatrix(instrumentList, ratioList, fromDate, toDate,
                                                               persistTempFile)
        return dictOfMatrix

    def subscribeTickData(self, instrument):
        # instrument has the broker to subscribe
        raise NotImplementedError
        pass

    def sendOrder(self, order):
        self.trading_service.sendOrder(order)

    def cancelOrder(self, order):
        self.trading_service.cancelOrder(order)

    def getAccountBalance(self, broker):
        self.trading_service.getAccountBalance(broker)
