from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.model.data_dict_keys import DataDictKeys
from tradeasystems_connector.service.vector_service import VectorService
from tradeasystems_connector.util.instrument_util import getSP500TickersBySector, getIbexTickersBySector, \
    getSharadarGroupsSector


def getDataDictOfMatrixAlphalensFromDict(dictOfMatrix, ratioList=[], cleanIt=True):
    import pandas as pd
    for key in dictOfMatrix:
        # dailyIndex
        indexDF = dictOfMatrix[key].index
        try:
            newIndex = pd.DatetimeIndex(indexDF, freq='D')
        except Exception as e:
            # logger.error('Error setting freq to D in %s' % key)
            newIndex = indexDF
        dictOfMatrix[key].set_index(newIndex, inplace=True)

        # removed items zero
        ratiosSum = dictOfMatrix[key].sum(axis=1)
        dictOfMatrix[key] = dictOfMatrix[key][ratiosSum != 0]

    prices = dictOfMatrix[DataDictKeys.close].fillna(0)
    factorDf = None
    for key in ratioList:
        series = getFactor(dictOfMatrix[key])
        if factorDf is None:
            factorDf = pd.DataFrame(0, index=series.index, columns=ratioList)
        factorDf[key] = series
    if cleanIt:
        factorDf, prices = cleanPricesFactor(factorDf, prices)

    return factorDf, prices


def __resetMultiIndexDF__(factorDf):
    factorDf_1 = factorDf.reset_index(level=1)
    factorDf_1 = factorDf_1.reset_index(level=0)
    output = factorDf_1.set_index(['index', 'level_1'])
    return output


# if cleanRows is True drop rows until all columns has no zeros
# if cleanRows is False drop all columns has zeros in any row
def cleanPricesFactor(factorDf, prices, cleanRows=True):
    import numpy as np
    logger.debug('Cleaning factorDF+prices to alphalens')
    # remove symbols all prices to zero,
    columnsStart = list(factorDf.index.levels[1])
    columnsRemove = list(prices.sum(axis=0)[prices.sum(axis=0) == 0].index)
    if len(columnsRemove) > 0:
        prices = prices.drop(columnsRemove, axis=1)
        factorDf = factorDf.drop(columnsRemove, axis=1, level=1)
        factorDf = __resetMultiIndexDF__(factorDf)
        columnsFinal = list(factorDf.index.levels[1])
        logger.debug('Cleaned columns factor/prices from %d to %d, removed :%s' % (
            len(columnsStart), len(columnsFinal), str(columnsRemove)))

    # rows with zero prices => remove it from factorDF
    if cleanRows:
        originalIndex = len(prices.index)
        originalFrom = str(prices.index[0])
        prices = prices.replace([np.inf, -np.inf, 0], np.nan)
        prices.dropna(axis=0, inplace=True)
        if prices.empty:
            logger.error('Prices are empty after cleaning')
        newIndexTime = prices.index
        newIndexFrom = str(newIndexTime[0])
        factorDf = factorDf.T[newIndexTime].T
        factorDf = __resetMultiIndexDF__(factorDf)

        logger.debug('Cleaned rows factor/prices from[%d] %s to[%d] %s' % (
            originalIndex, originalFrom, len(newIndexTime), newIndexFrom))
    # columns with zero prices=> remove it
    else:
        prices = prices.replace([np.inf, -np.inf, 0], np.nan)
        prices.dropna(axis=1, inplace=True)
        columnsRemove = [x for x in list(factorDf.index.levels[1]) if x not in list(prices.columns)]
        factorDf = factorDf.drop(columnsRemove, axis=0, level=1)
        factorDf = __resetMultiIndexDF__(factorDf)

        logger.debug('Cleaned columns factor/prices %s' % str(columnsRemove))
    logger.debug('Cleaned prices shape =  %d rows and %d  columns' % (prices.shape[0], prices.shape[1]))
    return factorDf, prices


def add_freq(idx, freq=None):
    import pandas as pd
    """Add a frequency attribute to idx, through inference or directly.

    Returns a copy.  If `freq` is None, it is inferred.
    """

    idx = idx.copy()
    if freq is None:
        if idx.freq is None:
            freq = pd.infer_freq(idx)
        else:
            return idx
    idx.freq = pd.tseries.frequencies.to_offset(freq)
    if idx.freq is None:
        raise AttributeError('no discernible frequency found to `idx`.  Specify'
                             ' a frequency string with `freq`.')
    return idx


def getFactor(dataframe, rankIt=True):
    # newIndex = pd.DatetimeIndex(dataframe.index , freq='B' )
    # dataframe = pd.DataFrame(dataframe.values, columns=dataframe.columns, index = newIndex)

    output = dataframe.stack()
    # output.index.freq = 'C'#is custom , only business dates

    # to fix issue https://github.com/quantopian/alphalens/issues/131
    if rankIt:
        output = output.rank(method='first')
    return output


def getSP500GroupsSector(symbolList):
    sp500ByGroups = getSP500TickersBySector()

    output = {k: v for k, v in zip(symbolList, ['group'] * len(symbolList))}

    for group in sp500ByGroups.keys():
        stocksList = sp500ByGroups[group]
        for stock in stocksList:
            if stock in output.keys():
                output[stock] = group
    return output


def getIBEXGroups(symbolList):
    ibexByGroups = getIbexTickersBySector()

    output = {k: v for k, v in zip(symbolList, ['group'] * len(symbolList))}

    for group in ibexByGroups.keys():
        stocksList = ibexByGroups[group]
        for stock in stocksList:
            if stock in output.keys():
                output[stock] = group
    return output


class FactorService:
    vectorizedDataService = None
    user_settings = None

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.vectorizedDataService = VectorService(self.user_settings)
        # self.vectorizedDataService.force_download = False

    def getDataDictOfMatrix(self, instrumentList, ratioList, fromDate, toDate=None, persistTempFile=None):
        dictOfMatrix = self.vectorizedDataService.getDataDictOfMatrix(instrumentList, ratioList, fromDate, toDate,
                                                                      persistTempFile)

        return dictOfMatrix

    def getDataDictOfMatrixAlphalens(self, instrumentList, ratioList, fromDate, toDate=None, persistTempFile=None):
        dictOfMatrix = self.getDataDictOfMatrix(instrumentList, ratioList, fromDate, toDate, persistTempFile)
        return getDataDictOfMatrixAlphalensFromDict(dictOfMatrix=dictOfMatrix, ratioList=ratioList)

    def getFactor(self, factorDict, key):
        return getFactor(factorDict[key])

    # def getSP500GroupsSubIndustry(self,symbolList):
    #     sp500ByGroups = getSP500TickersBySubIndustry()
    #
    #     pass
    def getSP500GroupsSector(self, symbolList):
        return getSP500GroupsSector(symbolList)

    def getSharadarGroupsSector(self, symbolList):
        return getSharadarGroupsSector(self.user_settings, symbolList)

    def getIBEXGroups(self, symbolList):
        return getIBEXGroups(symbolList)

    def divideKeys(self, dictOfMatrix, key_numerator, key_denominator):
        import numpy as np
        dataframeDivision = dictOfMatrix[key_numerator] / dictOfMatrix[key_denominator]
        dataframeDivision = dataframeDivision.replace([np.inf, -np.inf], 0)
        dataframeDivision.fillna(0, inplace=True)
        return dataframeDivision

    def multiplyKeys(self, dictOfMatrix, key_1, key_2):
        import numpy as np
        dataframeProduct = dictOfMatrix[key_1] / dictOfMatrix[key_2]
        dataframeProduct = dataframeProduct.replace([np.inf, -np.inf], 0)
        dataframeProduct.fillna(0, inplace=True)
        return dataframeProduct

    def filterKeysLower(self, dictOfMatrix, key, valueCondition=0):
        mask = dictOfMatrix[key] < valueCondition
        dictOfMatrix[~mask] = 0
