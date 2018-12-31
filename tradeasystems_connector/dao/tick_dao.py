from tradeasystems_connector.dao.pystore_dao import PyStoreDao
from tradeasystems_connector.model.tick import Tick


class TickDao(PyStoreDao):
    collectionName = 'tick'
    collection = None

    def __init__(self, user_settings):
        PyStoreDao.__init__(self, user_settings)

    def checkData(self, dataframe):
        correct = True
        # TODO
        for column in Tick.columns:
            if column == Tick.time:
                continue
            dataframe[column] = dataframe[column].astype(float)
        return correct

    def getCollectionName(self, instrument):
        collectionName = 'tick'
        return collectionName

    def getItemName(self, instrument):
        return '%s_%s' % (instrument.symbol.split()[0], instrument.currency)

    def save(self, dataframe, instrument):
        if self.checkData(dataframe):
            collectionName = self.getCollectionName(instrument)
            super().setCollectionVariables(collectionName)
            return super().save(self.getItemName(instrument), dataframe, instrument.asset_type, epochDate=True)
        else:
            return False

    def load(self, instrument, startTime=None, endTime=None):
        collectionName = self.getCollectionName(instrument)
        super().setCollectionVariables(collectionName)
        return super().load(self.getItemName(instrument), startTime, endTime)

    def delete(self, instrument):
        collectionName = self.getCollectionName(instrument)
        super().setCollectionVariables(collectionName)
        super().delete(self.getItemName(instrument))
