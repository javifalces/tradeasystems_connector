import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
import os
import time

import joblib
import pandas as pd

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.conf.region_settings import timezone_setting
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.bar_type import BarType
from tradeasystems_connector.model.data_dict_keys import DataDictKeys
from tradeasystems_connector.model.instrument import __stackInstrumentList__, __unstackInstrumentList__
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService
from tradeasystems_connector.service.ratio_data_service import RatioDataService
from tradeasystems_connector.util.configuration_keys_util import getTempPath
from tradeasystems_connector.util.date_util import convert_date
from tradeasystems_connector.util.parallel_processing import mpPandasObj
from tradeasystems_connector.util.persist_util import dataDict_to_excel


def setNewDateTimeIndexLoc(oldPandas, newPandas):
    if isinstance(oldPandas, pd.Series):
        output = pd.Series(None, newPandas.index)
    if isinstance(oldPandas, pd.DataFrame):
        output = pd.DataFrame(None, newPandas.index)

    newIndexInOldDataframe = newPandas.index.searchsorted(oldPandas.index)

    output[newIndexInOldDataframe] = oldPandas.values
    output.fillna(method='ffill', inplace=True)
    output.fillna(0, inplace=True)
    return output


class VectorOnlineService:
    historicalMarketDataService = None
    ratioDataService = None
    user_settings = None
    force_download = True  # if not found download it
    bar_to_vectorized_key = \
        {
            Bar.close: DataDictKeys.close,
            Bar.open: DataDictKeys.open,
            Bar.low: DataDictKeys.low,
            Bar.high: DataDictKeys.high,
            Bar.volume: DataDictKeys.volume,
        }
    # temp folder cache
    temp_dir = None
    cacher = None
    temp_dir_data_downloaded = None
    # threads download matrix
    threads = 10  # 7*cpus
    useFunctionTemp = True
    parallelDownloadRatios = True  # just to debug
    parallelDownloadInstruments = True  # Errors :: rows with 0
    ratio_list_from_input_vectorized = {}

    # lock = None

    def __init__(self, user_settings):
        self.user_settings = user_settings

        self.historicalMarketDataService = HistoricalMarketDataService(self.user_settings)
        self.ratioDataService = RatioDataService(self.user_settings)
        self.temp_dir = getTempPath(userSettings=user_settings)
        self.cacher = joblib.Memory(self.temp_dir)

        self.temp_dir_data_downloaded = getTempPath(
            self.user_settings) + os.sep + 'ratio_list_from_input_vectorized.pkl'
        if os.path.isfile(self.temp_dir_data_downloaded):
            self.ratio_list_from_input_vectorized = joblib.load(self.temp_dir_data_downloaded)

        # self.lock = Lock()
        pass

    def formatTime(self, timeColumn):
        import pandas as pd
        # The price comes from the daily info -
        #  so it would be the price at the end of the day GMT based on the requested TS

        # http://pvlib-python.readthedocs.io/en/latest/timetimezones.html
        originTZ = 'Etc/GMT'

        datetimeSeries = pd.to_datetime(timeColumn)

        return pd.DatetimeIndex(pd.to_datetime(datetimeSeries, unit='ms')).tz_localize(originTZ).tz_convert(
            timezone_setting)

    def getFactor(self, factorDict, key):
        return factorDict[key].stack()

    def getLongestRatioListDownloaded(self, instrumentList, ratioList, fromDate, toDate):
        pk = self.__getInputKeys__(instrumentList, fromDate, toDate)
        logger.debug('Searching longest ratioList with pk : %s' % pk)
        if not pk in self.ratio_list_from_input_vectorized.keys():
            return ratioList
        else:
            ratioListTemp = self.ratio_list_from_input_vectorized[pk]
            allIn = True
            for ratio in ratioList:
                if ratio not in ratioListTemp:
                    allIn = False
                    logger.debug('%s not in temp ratioList' % ratio)
            if allIn:
                logger.debug('all ratios requested cached %s' % str(ratioList))
                ratioList = ratioListTemp

        return ratioList

    def __printInstrumentList__(self, instrumentList):
        return ", ".join(str(x) for x in instrumentList)

    def __getInputKeys__(self, instrumentList, fromDate, toDate):
        import hashlib
        import time

        start = time.time()
        output = '%s_%s_%s' % (str(fromDate), str(toDate), self.__printInstrumentList__(instrumentList))

        hash_object = hashlib.md5(str.encode(output))
        output = hash_object.hexdigest()
        # logger.info('Took %.3f minutes to __getInputKeys__' % ((time.time() - start) / 60))
        return output

    def getDataDictOfMatrix(self, instrumentList, ratioList, fromDate, toDate=None, persistTempFile=None,
                            ):
        fromDate = pd.datetime(day=fromDate.day, month=fromDate.month, year=fromDate.year)
        if toDate is None:
            toDate = pd.datetime.today()
        toDate = pd.datetime(day=toDate.day, month=toDate.month, year=toDate.year)
        if self.useFunctionTemp:
            logger.debug('getDataDictOfMatrix downloading/loading...')
            logger.debug('instrumentList:  %s' % instrumentList)
            logger.debug('ratioList:  %s' % ratioList)
            logger.debug('fromDate:  %s' % fromDate)
            logger.debug('toDate:  %s' % toDate)
            logger.debug('persistTempFile:  %s' % persistTempFile)

            functionTemp = self.cacher.cache(self.__getDataDictOfMatrix__, ignore=['self'])
            ratioList = self.getLongestRatioListDownloaded(instrumentList, ratioList, fromDate, toDate)
            outputDictFinal = functionTemp(__unstackInstrumentList__(instrumentList), ratioList, fromDate, toDate, None)
        else:
            logger.info('Not using cache function!')
            outputDictFinal = self.__getDataDictOfMatrix__(__unstackInstrumentList__(instrumentList), ratioList,
                                                           fromDate, toDate, None)

        if persistTempFile is False:
            persistTempFile = None

        if persistTempFile is not None:
            logger.debug('Persisting ')
            if ~persistTempFile.endswith('.xlsx'):
                persistTempFile += '.xlsx'
            self.__createTempExcelFile__(outputDictFinal, filenameWithExtension=persistTempFile)
        self.__save_ratio_list__(instrumentList, ratioList, fromDate, toDate)
        return outputDictFinal

    def __save_ratio_list__(self, instrumentList, ratioList, fromDate, toDate):
        pk = self.__getInputKeys__(instrumentList, fromDate, toDate)
        if pk not in self.ratio_list_from_input_vectorized.keys():
            logger.debug('Saving cacheRatioList %s' % str(ratioList))
            self.ratio_list_from_input_vectorized[pk] = ratioList
            joblib.dump(self.ratio_list_from_input_vectorized, self.temp_dir_data_downloaded)
        else:
            previous_ratioList = self.ratio_list_from_input_vectorized[pk]
            if len(ratioList) > len(previous_ratioList):
                logger.debug('Saving/updating cacheRatioList %s' % str(ratioList))
                self.ratio_list_from_input_vectorized[pk] = ratioList
                joblib.dump(self.ratio_list_from_input_vectorized, self.temp_dir_data_downloaded)

    def __cleanOutputDict__(self, outputDict, wrongInstrumentsSymbolLists):
        if len(wrongInstrumentsSymbolLists) == 0:
            return outputDict
        logger.debug('Some errors detected in %s => cleaning' % str(wrongInstrumentsSymbolLists))
        keysInDict = outputDict.keys()
        for key in keysInDict:
            dataframeToRemoveColumns = outputDict[key]
            try:
                dataframeToRemoveColumns.drop(wrongInstrumentsSymbolLists, axis=1, inplace=True)
                outputDict[key] = dataframeToRemoveColumns
            except:
                pass
        return outputDict

    def __cleanBankHolidays__(self, outputDict):
        import pandas as pd
        from pandas.tseries.offsets import BDay
        isBusinessDay = BDay().onOffset

        keysInDict = outputDict.keys()
        for key in keysInDict:
            # clean the bank holidays+weekends
            match_series = pd.to_datetime(outputDict[key].index).map(isBusinessDay)
            outputDict[key] = outputDict[key][match_series]
        return outputDict

    def __downloadHistoricalPrices__(self, wrongInstrumentsSymbolLists, outputDict, instrument, fromDate, toDate):
        # self.lock.acquire()
        # outputDict['wrong'] = wrongInstrumentsSymbolLists
        historicalData = self.historicalMarketDataService.getHistoricalData(instrument,
                                                                            period=Period.day,
                                                                            number_of_periods=1,
                                                                            fromDate=fromDate,
                                                                            toDate=toDate,
                                                                            bar_type=BarType.time_bar,
                                                                            force_download=self.force_download,
                                                                            endOfDayData=True
                                                                            # force to download it with index time to zero
                                                                            )

        if historicalData is None:
            logger.error('%s_%s has no historical Data to vectorized => removing column' % (
                instrument.symbol, instrument.currency))
            wrongInstrumentsSymbolLists.append(instrument.symbol)
            if 'wrong' in outputDict.keys():
                outputDict['wrong'] += wrongInstrumentsSymbolLists
            else:
                outputDict['wrong'] = wrongInstrumentsSymbolLists
            # self.lock.release()
            return

        self.__formatPrices__(outputDict, instrument, historicalData)
        # self.lock.release()

    def __processInstrumentToVectorize__(self, instrument, ratioList, fromDate, toDate, outputDict,
                                         wrongInstrumentsSymbolLists):

        import warnings
        warnings.filterwarnings("ignore")
        if ratioList is None:
            ratioList = []
        else:
            logger.debug('gettings %s ratios %s to matrix' % (str(instrument), str(ratioList)))

        self.__downloadHistoricalPrices__(wrongInstrumentsSymbolLists, outputDict, instrument, fromDate=fromDate,
                                          toDate=toDate)

        self.__downloadAllRatiosBatch__(outputDict, instrument, fromDate, toDate, ratioList,
                                        wrongInstrumentsSymbolLists)
        # for column in outputDict.keys():
        #     if column == 'wrong':
        #         continue
        #     outputDict[column].fillna(method='ffill', inplace=True)
        #     # remove duplicated
        #     outputDict[column] = outputDict[column][~outputDict[column].index.duplicated(keep='last')]
        #
        #     # outputDict[column].fillna(0, inplace=True)
        #     # remove duplicated

        return outputDict

    def __formatPrices__(self, outputDict, instrument, historicalData):
        if historicalData is None:
            return None
        nanDF = self.__createDataframe__([instrument], fromDate=historicalData.index[0],
                                         toDate=historicalData.index[-1])
        for column in historicalData.columns:
            if self.bar_to_vectorized_key[column] not in outputDict.keys():
                outputDict[self.bar_to_vectorized_key[column]] = nanDF.copy()

            outputDict[self.bar_to_vectorized_key[column]][instrument.symbol] = historicalData[column]
        # return outputDict

    def __downloadAllRatiosSerial__(self, outputDict, instrument, fromDate, toDate, ratioList,
                                    wrongInstrumentsSymbolLists, nanDF):
        import datetime
        for ratio in ratioList:
            if ratio.endswith('Y'):
                fromDateRatio = fromDate - datetime.timedelta(days=2 * 365)
            else:
                fromDateRatio = fromDate - datetime.timedelta(days=1 * 365)
            ratioData = self.ratioDataService.getRatioData(instrument, ratio, fromDateRatio, toDate,
                                                           force_download=self.force_download)
            if ratioData is None:
                logger.error(
                    '%s_%s has no ratio %s Data to vectorized =>  removing column' % (
                        instrument.symbol, instrument.currency, ratio))
                wrongInstrumentsSymbolLists.append(instrument.symbol)
            if ratio not in outputDict.keys():
                outputDict[ratio] = nanDF
                if ratioData is not None:
                    ratioIndexBar = nanDF.index.searchsorted(ratioData.index)
            else:
                if ratioData is not None:
                    ratioIndexBar = outputDict[ratio].index.searchsorted(ratioData.index)

            if ratioData is not None:
                ratioIndexBar[ratioIndexBar > 0] = ratioIndexBar[ratioIndexBar > 0] - 1  # Put first value
                # SettingWithCopyWarning:
                # A value is trying to be set on a copy of a slice from a DataFrame
                numpyValues = ratioData[Ratio.ratio].values.copy()
                if instrument.symbol not in outputDict[ratio].columns:
                    outputDict[ratio][instrument.symbol] = None
                outputDict[ratio][instrument.symbol][ratioIndexBar] = numpyValues

            # remove duplicated
            # outputDict[ratio][instrument.symbol].fillna(method='ffill', inplace=True)

            # outputDict[ratio][instrument.symbol].fillna(0, inplace=True)  # 1st row
        outputDict['wrong'] = wrongInstrumentsSymbolLists
        outputDict['wrong'] = list(set(outputDict['wrong']))
        # return outputDict

    def __downloadAllRatiosBatch__(self, outputDict, instrument, fromDate, toDate, ratioList,
                                   wrongInstrumentsSymbolLists):
        import datetime
        ratio = ratioList[0]
        if ratio.endswith('Y'):
            fromDateRatio = fromDate - datetime.timedelta(days=2 * 365)
        else:
            fromDateRatio = fromDate - datetime.timedelta(days=1 * 365)

        ratioDataBatch = self.ratioDataService.getRatioDataBatch(instrument, ratioList, fromDateRatio, toDate,
                                                                 force_download=self.force_download)

        if ratioDataBatch is None:
            logger.error(
                '%s_%s has no ratios %s Data to vectorized =>  removing column' % (
                    instrument.symbol, instrument.currency, ratioList))
            wrongInstrumentsSymbolLists.append(instrument.symbol)
        if (DataDictKeys.close in outputDict.keys()):
            indexSelected = outputDict[DataDictKeys.close].index
            nanDF = self.__createDataframe__([instrument], index=indexSelected)
        else:
            nanDF = self.__createDataframe__([instrument], fromDate=fromDate, toDate=toDate)

        for ratio in ratioList:
            if ratio not in outputDict.keys():
                outputDict[ratio] = nanDF.copy()
                if ratioDataBatch is not None:
                    ratioIndexBar = nanDF.copy().index.searchsorted(ratioDataBatch.index)
            else:
                if ratioDataBatch is not None:
                    ratioIndexBar = outputDict[ratio].index.searchsorted(ratioDataBatch.index)

            if ratioDataBatch is not None:
                ratioIndexBar[ratioIndexBar > 0] = ratioIndexBar[ratioIndexBar > 0] - 1  # Put first value
                # SettingWithCopyWarning:
                # A value is trying to be set on a copy of a slice from a DataFrame
                numpyValues = ratioDataBatch[ratio].values.copy()
                if instrument.symbol not in outputDict[ratio].columns:
                    outputDict[ratio][instrument.symbol] = None
                outputDict[ratio][instrument.symbol][ratioIndexBar] = numpyValues
                # outputDict[ratio].fillna(method='ffill',inplace=True)

        if 'wrong' in outputDict.keys():
            outputDict['wrong'] += wrongInstrumentsSymbolLists
        else:
            outputDict['wrong'] = wrongInstrumentsSymbolLists

        outputDict['wrong'] = list(set(outputDict['wrong']))
        # return outputDict

    def __createDataframe__(self, instrumentList, fromDate=None, toDate=None, index=None):
        import datetime
        if fromDate is None and index is None:
            raise Exception('need index or fromDate at least in __createDataframe__(vector_online_service)')
        symbolList = [instrument.symbol for instrument in instrumentList]
        if index is not None and isinstance(index, pd.DatetimeIndex):
            dateRange = index
        else:
            if toDate is None:
                toDate = datetime.datetime.today()
            dateRange = pd.date_range(start=fromDate, end=toDate, freq='D')
        # Add hours and timezone
        # dateRange = self.formatTime(dateRange)
        # hour, minute = getCloseTimeHourMinInUTC(instrumentList[0])
        # dateRange = dateRange + pd.DateOffset(hours=hour, minutes=minute)
        nanDF = pd.DataFrame(None, columns=symbolList, index=dateRange)
        return nanDF

    def __getDataDictOfMatrix__(self, instrumentStringStacked, ratioList, fromDate, toDate=None, persistTempFile=None):
        # with self.lock:
        logger.debug('__getDataDictOfMatrix__ downloading...')
        instrumentList = __stackInstrumentList__(instrumentStringStacked[0], instrumentStringStacked[1],
                                                 instrumentStringStacked[2])
        if ratioList is None:
            ratioList = []
        # compatibility
        if persistTempFile is False:
            persistTempFile = None

        fromDate = convert_date(fromDate)
        toDate = convert_date(toDate)
        assetType = instrumentList[0].asset_type

        wrongInstrumentsSymbolLists = []
        start = time.time()
        typeDownload = ''
        if self.parallelDownloadInstruments and len(instrumentList) > self.threads:
            mpBatches = float(len(instrumentList)) / float(self.threads)
            mpBatches = min(int(mpBatches / 5), 50)
            mpBatches = 1
            logger.info(
                'Downloading Data using parallel[%d threads ,%d mpBatches] __processAllInstruments__ of %d instruments  and %d ratios' % (
                    self.threads, mpBatches, len(instrumentList), len(ratioList)))

            outputDict = mpPandasObj(func=self.__processAllInstruments__, pdObj=('instrumentList', instrumentList),
                                     isVerticalParallel=True,
                                     numThreads=self.threads, mpBatches=mpBatches,
                                     linMols=True,
                                     ratioList=ratioList,
                                     fromDate=fromDate,
                                     toDate=toDate,
                                     wrongInstrumentsSymbolLists=wrongInstrumentsSymbolLists,
                                     )
            typeDownload = 'parallel'
        else:
            logger.info('Downloading Data using serial __processAllInstruments__ of %d instruments  and %d ratios' % (
                len(instrumentList), len(ratioList)))
            outputDict = self.__processAllInstruments__(instrumentList, ratioList, fromDate, toDate,
                                                        wrongInstrumentsSymbolLists)
            typeDownload = 'serial'
        end = time.time()
        logger.info('******')
        minutesTime = (end - start) / 60
        logger.info('Took %f minutes to finish %s __getDataDictOfMatrix__' % (minutesTime, typeDownload))
        logger.info('******')
        dateFinal = None
        columnsFinalRemove = None
        for keys in outputDict.keys():
            if keys == 'wrong':
                continue
            outputDict[keys] = outputDict[keys][~outputDict[keys].index.duplicated(keep='last')]
            dateIndex = outputDict[keys][(outputDict[keys].fillna(0).sum(axis=1) != 0)].index
            # columnsClean = list(outputDict[keys].columns[outputDict[keys].fillna(0).sum()==0])
            if len(dateIndex) == 0:
                dateIndex = outputDict[keys].index
            # if len(columnsClean) ==0:
            #     del outputDict[keys]
            #     continue

            if dateFinal is None:
                dateFinal = dateIndex
            else:
                dateFinal = dateFinal.intersection(dateIndex)

            # if columnsFinalRemove is None:
            #     columnsFinalRemove = columnsClean
            # else:
            #     columnsFinalRemove=list(set(columnsFinalRemove+columnsClean))

        for keys in outputDict.keys():
            if keys == 'wrong':
                continue
            # outputDict[keys].drop(columns = columnsFinalRemove,inplace=True)
            mask = dateFinal.searchsorted(outputDict[keys].index)
            outputDict[keys] = outputDict[keys][mask > 0]

            outputDict[keys].fillna(0, inplace=True)

        logger.debug('all instruments processed => cleaning ')
        wrongInstrumentsSymbolLists = outputDict['wrong'].copy()
        if 'wrong' in outputDict:
            del outputDict['wrong']
        wrongInstrumentsSymbolLists = self.__formatWrongInstrumentList__(wrongInstrumentsSymbolLists, outputDict)

        outputDictFinal = self.__cleanOutputDict__(outputDict, wrongInstrumentsSymbolLists)

        if assetType != AssetType.forex and assetType != AssetType.crypto:
            outputDictFinal = self.__cleanBankHolidays__(outputDictFinal)
        df = outputDictFinal[DataDictKeys.close]
        logger.debug(
            'all dictOfMatrix cleaned => finished %d matrixes of %d columns' % (len(outputDictFinal), df.shape[1]))

        if persistTempFile is not None:
            if ~persistTempFile.endswith('.xlsx'):
                persistTempFile += '.xlsx'
            self.__createTempExcelFile__(outputDictFinal, filenameWithExtension=persistTempFile)
        return outputDictFinal

    def __processInstrument__(self, instrument, ratioList, fromDate, toDate, wrongInstrumentsSymbolLists,
                              outputDict):
        logger.debug('%s_%s processing to vectorize ' % (instrument.symbol, instrument.currency))
        if self.parallelDownloadInstruments is True and self.parallelDownloadRatios is True:
            self.parallelDownloadRatios = False
        if self.parallelDownloadRatios and len(ratioList) > self.threads:
            logger.info('__processInstrument__ parallel into %d threads ' % self.threads)
            outputDict = mpPandasObj(func=self.__processInstrumentToVectorize__, pdObj=('ratioList', ratioList),
                                     numThreads=self.threads, mpBatches=1, isVerticalParallel=True,
                                     instrument=instrument,
                                     # ratioList=ratioList,
                                     fromDate=fromDate,
                                     toDate=toDate,
                                     outputDict=outputDict,
                                     wrongInstrumentsSymbolLists=wrongInstrumentsSymbolLists,
                                     )
        else:
            # logger.info('__processInstrument__ serialized ' )
            outputDict = self.__processInstrumentToVectorize__(instrument, ratioList, fromDate, toDate, outputDict,
                                                               wrongInstrumentsSymbolLists)

        return outputDict

    def __processAllInstruments__(self, instrumentList, ratioList, fromDate, toDate, wrongInstrumentsSymbolLists
                                  ):
        from tqdm import tqdm
        outputDict = {}
        for instrument in tqdm(instrumentList):
            outputDict = self.__processInstrument__(instrument, ratioList, fromDate, toDate,
                                                    wrongInstrumentsSymbolLists, outputDict)

        # %% clean it
        for column in outputDict.keys():
            if column == 'wrong':
                continue
            outputDict[column].fillna(method='ffill', inplace=True)
            # remove duplicated
            outputDict[column] = outputDict[column][~outputDict[column].index.duplicated(keep='last')]

            # outputDict[column].fillna(0, inplace=True)
            # remove duplicated

        return outputDict

    def __formatWrongInstrumentList__(self, wrongInstrumentsSymbolLists, outputDict):
        df = outputDict[DataDictKeys.close]
        symbolList = list(df.columns)
        output = []
        for element in wrongInstrumentsSymbolLists:
            if isinstance(element, str) and element in symbolList:
                output.append(element)
            elif isinstance(element, list):
                for eachElement in element:
                    if isinstance(eachElement, str) and eachElement in symbolList:
                        output.append(eachElement)

        output = list(set(output))
        logger.debug('cleaning \nwrongInstrumentsSymbolLists: %s\noutput: %s' % (wrongInstrumentsSymbolLists, output))
        return output

    def __createTempExcelFile__(self, outputDictFinal, filenameWithExtension='DictOfMatrix.xlsx'):
        import os
        if outputDictFinal is not None and len(outputDictFinal) > 0:
            filenamePath = self.temp_dir + os.sep + filenameWithExtension
            logger.debug('saving matrixes to excel file : %s' % filenamePath)

            dataDict_to_excel(outputDictFinal, filenamePath)
