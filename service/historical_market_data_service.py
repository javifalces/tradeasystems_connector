import numpy as np
import pandas as pd
from numba import jit

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.dao.bar_dao import BarDao
from tradeasystems_connector.dao.tick_dao import TickDao
from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.bar_type import BarType
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.model.tick import Tick
from tradeasystems_connector.service.router_provider import RouterProvider
from tradeasystems_connector.util.date_util import convert_date


@jit(nopython=True)
def mad_outlier(y, thresh=3.):
    '''
    compute outliers based on mad
    # args
        y: assumed to be array with shape (N,1)
        thresh: float()
    # returns
        array index of outliers
    '''
    median = np.median(y)
    diff = np.sum((y - median) ** 2, axis=-1)
    diff = np.sqrt(diff)
    med_abs_deviation = np.median(diff)

    modified_z_score = 0.6745 * diff / med_abs_deviation

    return modified_z_score > thresh


class HistoricalMarketDataService:
    user_settings = None
    routerProvider = None
    tresholdDaysUpdate = 5
    temp_dir_data_downloaded = None

    # instrument_from_dict_date = {}

    # TODO save only at the end
    # def __del__(self):
    #     self.__saveDictFromDate__()

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.routerProvider = RouterProvider(self.user_settings)
        # self.temp_dir_data_downloaded = getTempPath(self.user_settings) + os.sep + 'instrument_from_dict_date.pkl'
        # if os.path.isfile(self.temp_dir_data_downloaded):
        # self.instrument_from_dict_date = joblib.load(self.temp_dir_data_downloaded)

    def __getKeyDict__(self, instrument):
        return '%s_%s_%s' % (instrument.symbol, instrument.currency, instrument.asset_type)

    # def __saveDictFromDate__(self):
    #     joblib.dump(self.instrument_from_dict_date, self.temp_dir_data_downloaded)
    # not should be used directly!
    def __getHistoricalDataProvider__(self, instrument, period, number_of_periods, fromDate, toDate=None,
                                      bar_type=BarType.time_bar):

        if bar_type != BarType.time_bar:
            # ToDo get from tick and calculate
            raise NotImplementedError

        historical_market_data = self.routerProvider.getHistoricalMarketDataProvider(instrument)
        historical_market_data = historical_market_data(self.user_settings)
        return historical_market_data.download(instrument, period, number_of_periods, fromDate, toDate)

    def __getHistoricalDataDB__(self, instrument: Instrument, period: Period, number_of_periods: int, fromDate,
                                toDate=None,
                                bar_type=BarType.time_bar):
        if period != Period.tick:
            dao = BarDao(self.user_settings)
            output = dao.load(instrument, period, number_of_periods, fromDate, toDate, bar_type)
        else:
            dao = TickDao(self.user_settings)
            output = dao.load(instrument, fromDate, toDate)

        return output

    def getHistoricalData(self, instrument, period, number_of_periods, fromDate, toDate=None,
                          bar_type=BarType.time_bar, force_download=False, endOfDayData=False, cleanOutliers=True):
        fromDate = convert_date(fromDate)
        toDate = convert_date(toDate)
        df_temp = None
        df = self.__getHistoricalDataDB__(instrument, period, number_of_periods, fromDate, toDate, bar_type)
        fromDateDictTemp = None

        # if self.__getKeyDict__(instrument) in self.instrument_from_dict_date:
        #     fromDateDictTemp = self.instrument_from_dict_date[self.__getKeyDict__(instrument)]

        if df is not None:
            if (df.index[0] - fromDate).days > self.tresholdDaysUpdate and force_download:
                if fromDateDictTemp is not None and fromDateDictTemp <= fromDate:
                    logger.debug(
                        'max downloaded ,cant download more to %s getting %d rows' % (instrument.symbol, len(df)))
                else:
                    # messageError = 'some data missing begining %s fromDateDictTemp=%s  fromDate=%s' % (
                    # instrument.symbol, str(fromDateDictTemp), str(fromDate))
                    # logger.error(messageError)
                    df_temp = df.copy()
                    df = None
            elif (toDate - df.index[-1]).days > self.tresholdDaysUpdate and force_download:
                logger.error('some data missing the end %s' % instrument.symbol)
                df_temp = df.copy()
                df = None
        if df is None and force_download:
            logger.debug('data not found in database trying to download and save it in %s!' % instrument.symbol)
            df_to_save = self.getHistoricalDataProvider(instrument, period, number_of_periods, fromDate, toDate,
                                                        bar_type)
            if df_to_save is not None:
                self.saveBarDataFrame(df_to_save, instrument, period, number_of_periods, bar_type)

            df = self.__getHistoricalDataDB__(instrument, period, number_of_periods, fromDate, toDate, bar_type)
        if df is None:
            if df_temp is not None:
                logger.debug('marketData: Cant download more , restoring DDBB files ')
                df = df_temp
            else:
                logger.error("Error getting historical data for %s_%s" % (instrument.symbol, instrument.currency))

        if fromDateDictTemp is not None:
            fromDateSave = min(fromDate, fromDateDictTemp)
        else:
            fromDateSave = fromDate

        # if fromDateSave != fromDateDictTemp:
        # self.instrument_from_dict_date[self.__getKeyDict__(instrument)] = fromDateSave  # add to dict and save
        # self.__saveDictFromDate__()  # save from date cache to file
        if cleanOutliers is True:
            df = self.cleanOutliersData(instrument, period, df)  # None checked inside

        if endOfDayData is True:
            df = self.__formatDatesEndOfDay__(df, fromDate, toDate)
        return df

    def __formatDatesEndOfDay__(self, df, fromDate, toDate):
        if df is None:
            return None
        dateRange = pd.date_range(start=fromDate, end=toDate, freq='D')
        output = pd.DataFrame(None, columns=df.columns, index=dateRange)
        logger.debug('getHistoricalData [endOfDayData]: Setting time to EOD ')
        for column in df.columns:
            closesIndexBar = output.index.searchsorted(df.index) - 1
            output[column][closesIndexBar] = df[column].values
        output.dropna(inplace=True)
        return output

    def getHistoricalDataProvider(self, instrument, period, number_of_periods, fromDate, toDate=None,
                                  bar_type=BarType.time_bar):
        df_provicer = self.__getHistoricalDataProvider__(instrument, period, number_of_periods, fromDate, toDate,
                                                         bar_type)
        return df_provicer

    def cleanOutliersData(self, instrument, period, df):
        if df is None:
            return None
        # TODO improve to get average of surrounded instead of delete
        output = df.copy()
        if period == Period.tick:
            wrongBid = mad_outlier(df[Tick.bid].values.reshape(-1, 1))

            output = df.loc[~wrongBid]
            wrongAsk = mad_outlier(df[Tick.ask].values.reshape(-1, 1))
            output = df.loc[~wrongAsk]
        else:
            wrongClose = mad_outlier(df[Bar.close].values.reshape(-1, 1))
            output = df.loc[~wrongClose]
            wrongOpen = mad_outlier(df[Bar.open].values.reshape(-1, 1))
            output = df.loc[~wrongOpen]
        if len(output) == 0:
            logger.error("All data was wrong for %s_%s when cleaning" % (instrument.symbol, instrument.currency))
        else:
            errorsFind = len(df) - len(output)
            if errorsFind is not 0:
                logger.debug('Find %i outliers to remove in %s_%s when cleaning' % (errorsFind,
                                                                                    instrument.symbol,
                                                                                    instrument.currency))
        return output

    def saveBarDataFrame(self, dataframe, instrument, period, number_of_periods, bar_type=BarType.time_bar):
        if period != Period.tick:
            dao = BarDao(self.user_settings)
            return dao.save(dataframe, instrument=instrument, period=period, number_of_periods=number_of_periods)
        else:
            return self.saveTickDataFrame(dataframe, instrument, period)

    def saveTickDataFrame(self, dataframe, instrument, period):
        if period == Period.tick:
            dao = TickDao(self.user_settings)
            return dao.save(dataframe, instrument=instrument)
        else:
            logger.error("Trying to save not tick period as tick ")
            return False

    def deleteBar(self, instrument, period, number_of_periods, bar_type=BarType.time_bar):
        dao = BarDao(self.user_settings)
        dao.delete(instrument=instrument, period=period, number_of_periods=number_of_periods, bar_type=bar_type)

        # if self.__getKeyDict__(instrument) in self.ratio_instrument_from_dict_date:
        #     self.ratio_instrument_from_dict_date.drop(self.__getKeyDict__(instrument))
        #     self.__saveDictFromDate__()
