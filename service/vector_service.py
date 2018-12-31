import datetime
import time

import joblib
import numpy as np
import pandas as pd
from tqdm import tqdm

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.dao.vector_dao import VectorDao
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.data_dict_keys import DataDictKeys
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.service.vector_online_service import VectorOnlineService
from tradeasystems_connector.util.configuration_keys_util import getTempPath
from tradeasystems_connector.util.date_util import convert_date
# save
# @jit(nopython=True,parallel=True)
from tradeasystems_connector.util.parallel_processing import mpPandasObj
from tradeasystems_connector.util.persist_util import save_to_file


def dictToDataframe(outputDict, symbol, columnList):
    dataframeSave = None
    for key in columnList:
        if key in outputDict.keys():
            if symbol in list(outputDict[key].columns):
                seriesFromDict = outputDict[key][symbol]
                seriesFromDict = seriesFromDict[~seriesFromDict.index.duplicated(keep='last')]
            else:
                # data not downloaded
                logger.debug('%s not found in %s => none' % (key, symbol))
                seriesFromDict = None
            if seriesFromDict is not None:
                if dataframeSave is None:
                    dataframeSave = pd.DataFrame(seriesFromDict.values, index=seriesFromDict.index, columns=[key])
                else:
                    dataframeSave[key] = seriesFromDict  # pd.Series(seriesFromDict.values,index = dataframeSave.index)
    if dataframeSave is not None:
        dataframeSave.fillna(method='ffill', inplace=True)
        # remove duplicated
        dataframeSave = dataframeSave[~dataframeSave.index.duplicated(keep='last')]

    return dataframeSave


# load
# @jit(nopython=True,parallel=True)
def dataframeToDict(dataframe, symbol, outputDict, columnList):
    for key in columnList:
        if key in list(dataframe.columns):
            if key in outputDict.keys():
                outputDict[key][symbol] = dataframe[key]
            else:
                series = dataframe[key]
                outputDict[key] = pd.DataFrame(series.values, columns=[symbol], index=series.index)
    return outputDict


class VectorService:
    vectorizedDataService = None
    vectorDao = None
    user_settings = None
    maxDaysUpdate = 3
    force_download = True  # if not found download it
    # cache function
    temp_dir = None
    cacher = None
    threads = 5  # 3,is modified later
    useFunctionTemp = False

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.vectorDao = VectorDao(self.user_settings)
        self.vectorizedDataService = VectorOnlineService(self.user_settings)
        self.vectorizedDataService.parallelDownloadInstruments = False
        if self.threads > 1:
            self.vectorizedDataService.parallelDownloadRatios = False
        else:
            self.vectorizedDataService.parallelDownloadRatios = False

        self.temp_dir = getTempPath(userSettings=user_settings)
        self.cacher = joblib.Memory(self.temp_dir, compress=9, verbose=0)

    def __loadDao__(self, instrument, outputDict, columnList, fromDate, toDate):

        toDate = toDate + datetime.timedelta(days=1)
        dataframe = self.vectorDao.load(instrument, columnList=columnList, startTime=fromDate, endTime=toDate)

        if dataframe is None:
            logger.debug('%s not found in dao' % instrument)
            return outputDict, dataframe

        if dataframe.dropna(axis=0).empty:
            logger.debug('%s is empty  => update with online data' % (instrument))
            return outputDict, None

        dataframeFrom = dataframe.index[0]
        dataframeTo = dataframe.index[-1]
        if (dataframeFrom - fromDate).days > self.maxDaysUpdate:
            logger.debug('fromDate = %s and get from=%s' % (fromDate, dataframeFrom))

        if (toDate - dataframeTo).days > self.maxDaysUpdate:
            logger.debug('toDate = %s and get from=%s => update it' % (toDate, dataframeTo))
            return outputDict, None

        outputDict = dataframeToDict(dataframe, instrument.symbol, outputDict, columnList)

        return outputDict, dataframe

    def __saveDao__(self, outputDict, instrument, columnList):
        dataframeSave = dictToDataframe(outputDict, instrument.symbol, columnList)
        output = self.vectorDao.save(dataframeSave, instrument)
        if output is False:
            logger.error('Cant save %s to vector database' % instrument)

    def __cleanData__(self, outputDict, assetType, persistTempFile=None):

        logger.debug('all instruments processed => cleaning ')
        if 'wrong' in outputDict.keys():
            wrongInstrumentsSymbolLists = outputDict['wrong'].copy()
            if 'wrong' in outputDict:
                del outputDict['wrong']
            wrongInstrumentsSymbolLists = self.vectorizedDataService.__formatWrongInstrumentList__(
                wrongInstrumentsSymbolLists, outputDict)

            outputDictFinal = self.vectorizedDataService.__cleanOutputDict__(outputDict, wrongInstrumentsSymbolLists)
        else:
            outputDictFinal = outputDict
        if assetType != AssetType.forex and assetType != AssetType.crypto:
            outputDictFinal = self.vectorizedDataService.__cleanBankHolidays__(outputDictFinal)
        df = outputDictFinal[DataDictKeys.close]
        logger.debug(
            'all dictOfMatrix cleaned => finished %d matrixes of %d columns' % (len(outputDictFinal), df.shape[1]))

        if persistTempFile is not None:
            if persistTempFile is False:
                persistTempFile = None
            if persistTempFile is True:
                persistTempFile = 'default.xlsx'

            if ~persistTempFile.endswith('.xlsx'):
                persistTempFile += '.xlsx'
            self.vectorizedDataService.__createTempExcelFile__(outputDictFinal, filenameWithExtension=persistTempFile)

        return outputDictFinal

    def __getInstrumentData__(self, instrument_symbol, instrument_currency, instrument_assetType, columnList,
                              fromDate, toDate, outputDict,
                              wrongInstrumentsSymbolLists):
        instrument = Instrument(symbol=instrument_symbol, currency=instrument_currency, asset_type=instrument_assetType)

        if self.useFunctionTemp:
            functionTemp = self.cacher.cache(self.___getInstrumentData___,
                                             ignore=['self', 'outputDict', 'wrongInstrumentsSymbolLists'])

            columnList.sort()
            functionTemp(instrument, columnList, fromDate, toDate, outputDict, wrongInstrumentsSymbolLists)
        else:
            # logger.debug('Not using functionTemp cache in __getInstrumentData__ in vector_service for %s' % instrument)
            self.___getInstrumentData___(instrument, columnList, fromDate, toDate, outputDict,
                                         wrongInstrumentsSymbolLists)

    def getRatioList(self, instrument):
        assetType = instrument.asset_type
        if assetType == AssetType.us_equity:
            return Ratio.allRatios
        else:
            return Ratio.quant_list

    def ___getInstrumentData___(self, instrument, columnList, fromDate, toDate, outputDict,
                                wrongInstrumentsSymbolLists):
        startTime = time.time()
        outputDict, dataframe = self.__loadDao__(instrument=instrument, outputDict=outputDict,
                                                 columnList=columnList, fromDate=fromDate, toDate=toDate)
        if self.force_download and dataframe is None:
            logger.debug('%s not found in vector database => generate all' % instrument)
            completeRatioList = self.getRatioList(instrument=instrument)

            outputDict = self.vectorizedDataService.__processInstrument__(instrument, completeRatioList, fromDate,
                                                                          toDate,
                                                                          wrongInstrumentsSymbolLists,
                                                                          outputDict)
            logger.debug('%s data generated => saving' % instrument)
            self.__saveDao__(outputDict, instrument, columnList)




        else:
            logger.debug('%s found in vector ddbb' % instrument)
        endTime = time.time()
        logger.debug('Took %d seconds to get Instrument %s' % ((endTime - startTime), instrument))

    def __getAllInstrumentData__(self, instrumentList, ratioList, fromDate, toDate):

        wrongInstrumentsSymbolLists = []
        # nanDF = self.vectorizedDataService.__createDataframe__(instrumentList, fromDate, toDate)
        columnList = DataDictKeys.keys + ratioList
        outputDict = {}
        if len(instrumentList) < self.threads:
            self.threads = int(np.ceil(len(instrumentList) / 2))
            logger.debug('Modified threads of __getAllInstrumentData__ to %d' % self.threads)
        if self.threads > 1:
            mpBatches = 1
            linMols = True
            if self.threads > 3:
                mpBatches = float(len(instrumentList)) / float(self.threads)
                mpBatches = int(min(int(mpBatches / 5), 50))
                if mpBatches < 1:
                    mpBatches = 1

            logger.debug('mpBatches of __getAllInstrumentData__ to %d' % mpBatches)
            # mpBatches = 1

            outputDict = mpPandasObj(func=self.__getAllInstrumentSerial__, pdObj=('instrumentList', instrumentList),
                                     numThreads=self.threads, mpBatches=mpBatches, isVerticalParallel=True,
                                     linMols=linMols,
                                     columnList=columnList,
                                     fromDate=fromDate,
                                     toDate=toDate,
                                     outputDict=outputDict,
                                     wrongInstrumentsSymbolLists=wrongInstrumentsSymbolLists,
                                     )
        else:
            logger.info('Downloading data serialized')
            outputDict = self.__getAllInstrumentSerial__(instrumentList, columnList, fromDate, toDate, outputDict,
                                                         wrongInstrumentsSymbolLists)

        for key in outputDict.keys():
            if isinstance(outputDict[key], pd.DataFrame):
                outputDict[key].fillna(method='ffill', inplace=True)
                outputDict[key].fillna(0, inplace=True)  # 1st row

        # filter to asked data
        if outputDict is not None:
            outputDictFinal = {}
            for ratioAsked in columnList:
                outputDictFinal[ratioAsked] = outputDict[ratioAsked]
        else:
            outputDictFinal = None

        return outputDictFinal

    def __getAllInstrumentSerial__(self, instrumentList, columnList, fromDate, toDate, outputDict,
                                   wrongInstrumentsSymbolLists):

        for instrument in tqdm(instrumentList):
            self.__getInstrumentData__(instrument.symbol, instrument.currency, instrument.asset_type,
                                       columnList, fromDate, toDate, outputDict, wrongInstrumentsSymbolLists,
                                       )

        return outputDict

    def __allignSymbolsDictMatrix__(self, dictMatrix):
        if dictMatrix is None:
            return None
        columnsClean = None
        for key in dictMatrix.keys():
            if columnsClean is None:
                columnsClean = list(dictMatrix[key].columns)
            columnsClean = set(columnsClean).intersection(list(dictMatrix[key].columns))
        columnsClean = list(dictMatrix[key][list(columnsClean)])
        columnsClean.sort()
        dictMatrixOut = {}
        for key in dictMatrix.keys():
            dictMatrixOut[key] = dictMatrix[key][list(columnsClean)]
        return dictMatrixOut

    def __cleanSymbolsDictMatrix__(self, dictMatrix):
        if dictMatrix is None:
            return None
        columnsToDrop = []
        # for key in dictMatrix.keys():
        # some fundamentals problem -> not data no check
        for key in DataDictKeys.keys:
            sumNan = dictMatrix[key].isnull().sum()
            lenTotal = dictMatrix[key].shape[0]
            percentageNan = sumNan / lenTotal

            cleanedDF = percentageNan[percentageNan > 0.8]
            if len(cleanedDF.index) > 0:
                columnsToDrop += list(cleanedDF.index)

        columnsToDrop = list(set(columnsToDrop))
        columnsToDrop.sort()
        dictMatrixOut = {}
        for key in dictMatrix.keys():
            dictMatrixOut[key] = dictMatrix[key]
            # columnsToDrop = set(dictMatrix[key].columns).difference(list(columnsToPersist))
            # columnsToDrop = set(columnsToDrop).intersection(list(dictMatrix[key].columns))
            if len(columnsToDrop) > 0:
                dictMatrixOut[key].drop(columnsToDrop, axis=1, inplace=True)
        return dictMatrixOut

    def getDataDictOfMatrix(self, instrumentList, ratioList, fromDate, toDate=None, persistTempFile=None):
        start = time.time()
        fromDate = pd.datetime(day=fromDate.day, month=fromDate.month, year=fromDate.year)
        if toDate is None:
            toDate = pd.datetime.today()
        toDate = pd.datetime(day=toDate.day, month=toDate.month, year=toDate.year)
        fromDate = convert_date(fromDate)
        toDate = convert_date(toDate)

        logger.debug('getDataDictOfMatrix downloading/loading...')
        logger.debug('instrumentList:  %s' % instrumentList)
        logger.debug('ratioList:  %s' % ratioList)
        logger.debug('fromDate:  %s' % fromDate)
        logger.debug('toDate:  %s' % toDate)
        logger.debug('persistTempFile:  %s' % persistTempFile)

        outputDict = self.__getAllInstrumentData__(instrumentList, ratioList, fromDate, toDate)

        assetType = instrumentList[0].asset_type
        outputDictFinal = self.__cleanData__(outputDict, assetType=assetType, persistTempFile=persistTempFile)

        end = time.time()
        logger.info('******')
        minutesTime = (end - start) / 60
        logger.info('Took %f minutes to finish __getDataDictOfMatrix__' % (minutesTime))
        logger.info('******')
        import os
        outputDictFinal = self.__allignSymbolsDictMatrix__(outputDictFinal)
        outputDictFinal = self.__cleanSymbolsDictMatrix__(outputDictFinal)
        logger.info('Finished => saving dictOfMatrix_last.pickle')
        save_to_file(outputDictFinal, getTempPath(self.user_settings) + os.sep + 'dictOfMatrix_last.pickle')
        return outputDictFinal
