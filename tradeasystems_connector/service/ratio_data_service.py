import pandas as pd

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.dao.ratio_dao import RatioDao
from tradeasystems_connector.dao.vector_dao import VectorDao
from tradeasystems_connector.quant_data.quant_data_impl import QuantDataImpl
from tradeasystems_connector.service.router_provider import RouterProvider
from tradeasystems_connector.util.date_util import convert_date, convertSerieClosestTimeIndex


class RatioDataService:
    user_settings = None
    routerProvider = None
    # ratio_instrument_from_dict_date = {}
    tresholdDaysUpdate = 5
    ratio_dao = None
    vector_dao = None

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.routerProvider = RouterProvider(self.user_settings)
        self.ratio_dao = RatioDao(self.user_settings)
        self.vector_dao = VectorDao(self.user_settings)

    # not should be used directly!
    def __getRatioDataProvider__(self, instrument, ratio, fromDate, toDate=None):
        if ratio.startswith('fundamental'):
            ratio_data = self.routerProvider.getFundamentalDataProvider(ratio, instrument)
        elif ratio.startswith('quant'):
            ratio_data = QuantDataImpl
        else:
            logger.error('uknown type of ratio: %s' % ratio)
            return None
        ratio_fundamental_data = ratio_data(self.user_settings)
        return ratio_fundamental_data.download(instrument, ratio, fromDate, toDate)

    def getRatioDataProviderBatch(self, instrument, ratio_list, fromDate, toDate=None):
        fundamental_ratios = []
        quant_ratios = []
        fundData = None
        quantData = None
        from tradeasystems_connector.conf.region_settings import timezone_setting
        indexTime = pd.date_range(start=fromDate, end=toDate, tz=timezone_setting)
        output = pd.DataFrame(None, columns=ratio_list, index=indexTime)

        for ratio in ratio_list:
            if ratio.startswith('fundamental'):
                fundamental_ratios.append(ratio)
                fundData = self.routerProvider.getFundamentalDataProvider(ratio, instrument)
            if ratio.startswith('quant'):
                quant_ratios.append(ratio)
                quantData = QuantDataImpl

        # Fundamentals
        if fundData is not None:
            fundamental_provider = fundData(self.user_settings)
            fundamental = fundamental_provider.downloadBatch(instrument, fundamental_ratios, fromDate, toDate)
            if fundamental is not None:
                output[fundamental_ratios] = fundamental[fundamental_ratios]

        # Quant ratios
        if quantData is not None:
            quant_provider = quantData(self.user_settings)
            for quant_ratio in quant_ratios:
                quantData = quant_provider.download(instrument, quant_ratio, fromDate, toDate)
                if quantData is None:
                    continue
                # output[quant_ratio] = quantData.ratio
                output[quant_ratio] = convertSerieClosestTimeIndex(quantData.ratio, output.index, shift=0).values
        output.fillna(method='ffill', inplace=True)
        return output

    def __getRatioDataDB__(self, instrument, ratio, fromDate, toDate=None):
        output = self.ratio_dao.load(instrument, ratio, fromDate, toDate)
        return output

    def __getRatioDataDBBatch__(self, instrument, ratio_list, fromDate, toDate=None):
        output = self.vector_dao.load(instrument, ratio_list, fromDate, toDate)
        return output

    # def __getKeyDict__(self, ratio, instrument):
    #     return '%s_%s_%s_%s' % (ratio, instrument.symbol, instrument.currency, instrument.asset_type)
    def getRatioDataBatch(self, instrument, ratio_list, fromDate, toDate=None, force_download=False):
        fromDate = convert_date(fromDate)
        toDate = convert_date(toDate)
        df_temp = None
        df = self.__getRatioDataDBBatch__(instrument, ratio_list, fromDate, toDate)
        fromDateDictTemp = None

        if df is not None:
            if (df.index[0] - fromDate).days > self.tresholdDaysUpdate and force_download:
                if fromDateDictTemp is not None and fromDateDictTemp <= fromDate:
                    logger.debug('max downloaded %s ,cant download more to %s' % (ratio_list, instrument))
                else:
                    # messageError = 'some data missing begining ratio %s in  %s fromDateDictTemp=%s  fromDate=%s' % (
                    # ratio, instrument.symbol, str(fromDateDictTemp), str(fromDate))
                    # logger.error(messageError)
                    df_temp = df.copy()
                    df = None

            elif (toDate - df.index[-1]).days > self.tresholdDaysUpdate and force_download:
                logger.debug('some data missing the end ')
                df_temp = df.copy()
                df = None
            elif (len(df.columns) < len(ratio_list) and force_download):
                logger.debug('some ratio  missing in database')
                df_temp = df.copy()
                df = None

        if df is None and force_download:
            logger.debug('data %s %s_%s not found in database trying to download and save it!' % (
                ratio_list, instrument.symbol, instrument.currency))
            df = self.getRatioDataProviderBatch(instrument, ratio_list, fromDate, toDate)
            # if df_to_save is not None:
            #     self.saveRatioDataFrameBatch(df_to_save, instrument, ratio_list)
            #     df = self.__getRatioDataDBBatch__(instrument, ratio_list, fromDate, toDate)
            # else:
            #     logger.error('Not pssible to download %s for %s' % (ratio_list, instrument))
            #     df = None

        if df is None:
            if df_temp is not None:
                logger.debug('ratioData: Cant download more , restoring DDBB files ')
                df = df_temp
            else:
                logger.error(
                    "Error getting ratio data for %s %s_%s" % (ratio_list, instrument.symbol, instrument.currency))
                return None
        if fromDateDictTemp is not None:
            fromDateSave = min(fromDate, fromDateDictTemp)
        else:
            fromDateSave = fromDate

        # if fromDateSave != fromDateDictTemp:
        #     self.ratio_instrument_from_dict_date[
        #         self.__getKeyDict__(ratio, instrument)] = fromDateSave  # add to dict and save
        #     self.__saveDictFromDate__()  # save from date cache to file

        return df

    def getRatioData(self, instrument, ratio, fromDate, toDate=None, force_download=False):

        fromDate = convert_date(fromDate)
        toDate = convert_date(toDate)
        df_temp = None

        df = self.__getRatioDataDB__(instrument, ratio, fromDate, toDate)
        fromDateDictTemp = None
        # if self.__getKeyDict__(ratio, instrument) in self.ratio_instrument_from_dict_date:
        #     fromDateDictTemp = self.ratio_instrument_from_dict_date[self.__getKeyDict__(ratio, instrument)]
        #     fromDateDictTemp = convert_date(fromDateDictTemp)

        if df is not None:
            if (df.index[0] - fromDate).days > self.tresholdDaysUpdate and force_download:
                if fromDateDictTemp is not None and fromDateDictTemp <= fromDate:
                    logger.debug('max downloaded %s ,cant download more to %s' % (ratio, instrument))
                else:
                    # messageError = 'some data missing begining ratio %s in  %s fromDateDictTemp=%s  fromDate=%s' % (
                    # ratio, instrument.symbol, str(fromDateDictTemp), str(fromDate))
                    # logger.error(messageError)
                    df_temp = df.copy()
                    df = None

            elif (toDate - df.index[-1]).days > self.tresholdDaysUpdate and force_download:
                logger.debug('some data missing the end ')
                df_temp = df.copy()
                df = None
        if df is None and force_download:
            logger.debug('data %s %s_%s not found in database trying to download and save it!' % (
                ratio, instrument.symbol, instrument.currency))
            df_to_save = self.getRatioDataProvider(instrument, ratio, fromDate, toDate)
            if df_to_save is not None:
                self.saveRatioDataFrame(df_to_save, instrument, ratio)
                df = self.__getRatioDataDB__(instrument, ratio, fromDate, toDate)
            else:
                logger.error('Not pssible to download %s for %s' % (ratio, instrument))
                df = None

        if df is None:
            if df_temp is not None:
                logger.debug('ratioData: Cant download more , restoring DDBB files ')
                df = df_temp
            else:
                logger.error("Error getting ratio data for %s %s_%s" % (ratio, instrument.symbol, instrument.currency))
                return None
        if fromDateDictTemp is not None:
            fromDateSave = min(fromDate, fromDateDictTemp)
        else:
            fromDateSave = fromDate

        # if fromDateSave != fromDateDictTemp:
        #     self.ratio_instrument_from_dict_date[
        #         self.__getKeyDict__(ratio, instrument)] = fromDateSave  # add to dict and save
        #     self.__saveDictFromDate__()  # save from date cache to file

        return df

    # def __saveDictFromDate__(self):
    #     joblib.dump(self.ratio_instrument_from_dict_date, self.temp_dir_data_downloaded)

    def getRatioDataProvider(self, instrument, ratio, fromDate, toDate=None):
        df_provicer = self.__getRatioDataProvider__(instrument, ratio, fromDate, toDate)
        return df_provicer

    def saveRatioDataFrame(self, dataframe, instrument, ratio):
        return self.ratio_dao.save(dataframe, instrument, ratio)

    def saveRatioDataFrameBatch(self, dataframe, instrument, ratio_list):
        return self.vector_dao.save(dataframe, instrument)

    def deleteRatio(self, instrument, ratio):
        self.ratio_dao.delete(instrument=instrument, ratio=ratio)

        # if self.__getKeyDict__(ratio, instrument) in self.ratio_instrument_from_dict_date:
        #     self.ratio_instrument_from_dict_date.drop(self.__getKeyDict__(ratio, instrument))
        #     self.__saveDictFromDate__()
