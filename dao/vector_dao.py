from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.dao.pystore_dao import PyStoreDao


class VectorDao(PyStoreDao):
    collectionName = 'vector'
    collection = None

    # Item = GOOG
    # dataframe columns = [close,open,high,low,accruals,debt.....]
    # dataframe index = [datetime from start to end]

    def __init__(self, user_settings):
        PyStoreDao.__init__(self, user_settings)

    def checkData(self, dataframe):
        import pandas as pd
        correct = True
        if not isinstance(dataframe, pd.DataFrame):
            correct = False
        else:
            if not isinstance(dataframe.index, pd.DatetimeIndex):
                correct = False
        return correct

    def getCollectionName(self):
        return self.collectionName

    def getItemName(self, instrument):
        return '%s' % (instrument)

    def mergeData(self, dataframe, oldDataframe, instrument):
        output = oldDataframe.copy()
        columnsNotInData = list(set(dataframe.columns).difference(list(output.columns)))
        if len(columnsNotInData) > 0:
            # for column in columnsNotInData:
            #     output[column] = dataframe[column][output.index]
            output[columnsNotInData] = dataframe[columnsNotInData]

            # delete previous all
            super().delete(self.getItemName(instrument))
        output = output.append(dataframe)
        return output

    def save(self, dataframe, instrument):
        if self.checkData(dataframe):
            collectionName = self.collectionName
            super().setCollectionVariables(collectionName)
            oldDataframe = self.load(instrument=instrument)
            if oldDataframe is None:
                newDataframe = dataframe
            else:
                newDataframe = self.mergeData(dataframe, oldDataframe, instrument)

            return super().save(self.getItemName(instrument), newDataframe, instrument.asset_type, epochDate=True)
        else:
            return False

    def load(self, instrument, columnList=None, startTime=None, endTime=None):
        collectionName = self.collectionName
        super().setCollectionVariables(collectionName)
        data = super().load(self.getItemName(instrument), startTime, endTime)
        if data is not None and columnList is not None:
            try:
                data = data[columnList]
                return data
            except Exception as e:
                logger.error('Error loading vector of %s columns %s  : %s' % (instrument, columnList, str(e)))
                data = None

        return data

    def delete(self, instrument, columnList=None):
        collectionName = self.collectionName
        super().setCollectionVariables(collectionName)
        oldData = self.load(instrument=instrument, columnList=None)
        if oldData is not None and columnList is not None:
            newdata = oldData.copy()
            columnsInData = list(set(newdata.columns).intersection(columnList))
            newdata.drop(columnsInData, axis=1, inplace=True)
            if not (newdata.empty):
                self.save(newdata, instrument)
        else:
            super().delete(self.getItemName(instrument))
