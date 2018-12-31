from tradeasystems_connector.dao.pystore_dao import PyStoreDao


class RatioDao(PyStoreDao):
    collectionName = 'ratio'
    collection = None

    def __init__(self, user_settings):
        PyStoreDao.__init__(self, user_settings)

    def checkData(self, dataframe):
        correct = True
        # TODO
        return correct

    def getCollectionName(self, instrument):
        collectionName = 'ratio'
        return collectionName

    def getItemName(self, instrument, ratio):
        return '%s_%s_%s' % (instrument.symbol.split()[0], instrument.currency, ratio)

    def save(self, dataframe, instrument, ratio):
        if self.checkData(dataframe):
            collectionName = self.getCollectionName(instrument)
            super().setCollectionVariables(collectionName)
            return super().save(self.getItemName(instrument, ratio), dataframe, instrument.asset_type, epochDate=True)
        else:
            return False

    def load(self, instrument, ratio, startTime=None, endTime=None):
        collectionName = self.getCollectionName(instrument)
        super().setCollectionVariables(collectionName)
        return super().load(self.getItemName(instrument, ratio), startTime, endTime)

    def delete(self, instrument, ratio):
        collectionName = self.getCollectionName(instrument)
        super().setCollectionVariables(collectionName)
        super().delete(self.getItemName(instrument, ratio))
