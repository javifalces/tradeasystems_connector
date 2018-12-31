from tradeasystems_connector.model.bar_type import BarType
from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService
from tradeasystems_connector.service.ratio_data_service import RatioDataService


class ManagerAdmin:
    user_settings = None
    historical_market_data_service = None
    ratio_data_service = None

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.historical_market_data_service = HistoricalMarketDataService(self.user_settings)
        self.ratio_data_service = RatioDataService(self.user_settings)

    def saveHistoricalDataProvider(self, instrument, period, number_of_periods, fromDate, toDate=None,
                                   bar_type=BarType.time_bar):
        # get data from historical_market_data and save into database
        self.historical_market_data_service.getHistoricalData(instrument, period, number_of_periods, fromDate,
                                                              toDate, bar_type, force_download=True)

    def saveRatioDataProvider(self, instrument, ratio, fromDate, toDate=None):
        # get data from historical_market_data and save into database
        self.ratio_data_service.getRatioData(instrument, ratio, fromDate,
                                             toDate, force_download=True)

    def subscribeSaveTickData(self, instrument):
        # subscribe to market from historical_market_data and save into database
        raise NotImplementedError

    # def saveHistoricalDataFile(self, instrument, period, number_of_periods, fromDate, toDate, file_source):
    #     # get data from historical_market_data and save into database format file
    #
    #     raise NotImplementedError
    #
    # def saveHistoricalTickFile(self, instrument, fromDate, toDate, file_source):
    #     # get data from historical_market_data and save into database
    #     raise NotImplementedError
