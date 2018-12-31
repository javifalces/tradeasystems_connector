import user_settings
from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.historical_market_data.dukascopy_file_historical_market_data import \
    DukasCopyFileHistoricalMarketData
from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService


class ManagerDukascopyImporter:
    user_settings = None
    market_data_provider = None
    historical_market_data_service = None

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.market_data_provider = DukasCopyFileHistoricalMarketData(user_settings)
        self.historical_market_data_service = HistoricalMarketDataService(self.user_settings)

    def importAllFiles(self):
        filesInInput = self.market_data_provider.filesInDirectory
        for file in filesInInput:
            instrument, fromDate, toDate, period, number_periods = self.market_data_provider.getDataFile_instrument_from_to_period_numberPeriods(
                file)
            dataDB = self.historical_market_data_service.getHistoricalData(instrument=instrument,
                                                                           number_of_periods=number_periods,
                                                                           fromDate=fromDate, toDate=toDate,
                                                                           period=period)
            if dataDB is not None:
                dataDB = dataDB[fromDate:toDate]
                if dataDB is not None and (not fromDate in dataDB.index or toDate not in dataDB.index):
                    dataDB = None

            if dataDB is None:
                # if dataDB is None:
                dataframe = self.market_data_provider.download(instrument, period, number_periods, fromDate, toDate)
                if dataframe is not None:
                    isSaved = self.historical_market_data_service.saveBarDataFrame(dataframe, instrument, period,
                                                                                   number_periods)
                    if isSaved:
                        self.market_data_provider.markFileAsProcessed(file)
                else:
                    # mark as proccessed if some errroes BID candlesstick for ex
                    logger.error('Cant read %s => mark as procesed' % file)
                    self.market_data_provider.markFileAsProcessed(file)
            else:
                # marked to avoid reading it next time
                self.market_data_provider.markFileAsProcessed(file)


def main(user_settings):
    manager = ManagerDukascopyImporter(user_settings)
    manager.importAllFiles()


if __name__ == "__main__":
    main(user_settings)
