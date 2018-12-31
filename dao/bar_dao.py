from tradeasystems_connector.dao.pystore_dao import PyStoreDao
from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.bar_type import BarType


class BarDao(PyStoreDao):
    collection = None
    collectionName = None

    def __init__(self, user_settings):
        PyStoreDao.__init__(self, user_settings)

    def getCollectionName(self, instrument, period, number_of_periods, bar_type=BarType.time_bar):
        # collectionName = '%s_%s_%s_%s_%i_%s' % (
        #     instrument.symbol, instrument.currency, instrument.asset_type, period, number_of_periods, bar_type)
        collectionName = '%s_%i_%s' % (period, number_of_periods, bar_type)
        return collectionName

    def checkData(self, dataframe):
        correct = True
        for column in Bar.columns:
            if column == Bar.time:
                continue
            dataframe[column] = dataframe[column].astype(float)
        # TODO
        return correct

    def getItemName(self, instrument):
        return '%s_%s' % (instrument.symbol.split()[0], instrument.currency)

    def save(self, dataframe, instrument, period, number_of_periods, bar_type=BarType.time_bar):
        if self.checkData(dataframe):
            collectionName = self.getCollectionName(instrument, period, number_of_periods, bar_type)
            super().setCollectionVariables(collectionName)

            return super().save(self.getItemName(instrument), dataframe, instrument.asset_type, epochDate=True)
        else:
            return False

    def load(self, instrument, period, number_of_periods, startTime=None, endTime=None, bar_type=BarType.time_bar):
        collectionName = self.getCollectionName(instrument, period, number_of_periods, bar_type)
        super().setCollectionVariables(collectionName)

        return super().load(self.getItemName(instrument), startTime, endTime)

    def delete(self, instrument, period, number_of_periods, bar_type=BarType.time_bar):
        collectionName = self.getCollectionName(instrument, period, number_of_periods, bar_type)
        super().setCollectionVariables(collectionName)

        super().delete(self.getItemName(instrument))
