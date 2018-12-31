import pystore
import numpy as np
from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.util.configuration_keys_util import getDatabasePath
import hashlib


class PyStoreDao:
    ### parent dao with pystore store config

    store = None

    collectionName = None
    collection = None

    cacheDict = {}

    # Fix to pystore
    #     in store.py def collection
    # return Collection(collection.split('\\')[1], self.datastore)
    def __init__(self, user_settings, storeName='AInvesting'):
        # List stores
        # pystore.list_stores()
        pystore.set_path(getDatabasePath(user_settings))
        # Connect to datastore (create it if not exist)
        self.store = pystore.store(storeName)
        self.cacheDict = {}  # Cache
        pass

    # Cache
    def __getMD5_key__(self, itemName, startTime, endTime):
        keyString = ''
        keyString += self.collectionName
        keyString += str(itemName)
        keyString += str(startTime)
        keyString += str(endTime)

        hash_object = hashlib.md5(str.encode(keyString))
        output = hash_object.hexdigest()
        return output

    # Cache
    def __load_cached__(self, itemName, startTime, endTime):
        key = self.__getMD5_key__(itemName, startTime, endTime)
        if key in self.cacheDict.keys():
            return self.cacheDict[key]
        else:
            return None

    def setCollectionVariables(self, collectionName):
        self.collectionName = collectionName
        self.collection = self.store.collection(self.collectionName, overwrite=False)

    def checkData(self, dataframe):
        if dataframe is None:
            return False
        return True

    def cleanDataframeDatetime(self, dataframe):
        import numpy as np
        if dataframe is None:
            return None
        dataframe_output = dataframe[~dataframe.index.duplicated(keep='last')]
        # dataframe_output = dataframe.drop_duplicates(subset='rownum', keep='last')
        dataframe_output = dataframe_output.sort_index(ascending=True)

        originalIndex = dataframe_output.index
        # newIndex =originalIndex
        try:
            newIndex = originalIndex.tz_localize('utc')
        except:
            newIndex = originalIndex.tz_convert('utc')
        # newIndex = np.array(newIndex.to_pydatetime(), dtype=np.datetime64)
        newIndex = np.array(newIndex, dtype=np.datetime64)
        dataframe_output.index = newIndex

        return dataframe_output

    def save(self, itemName, dataframe, source='uknown', epochDate=True):
        # Check update or insert
        output = False

        if self.checkData(dataframe):
            try:

                logger.debug("saving collection %s:  %s a dataframe of %d rows %d columns" % (
                    self.collectionName, itemName, dataframe.shape[0], dataframe.shape[1]))
                dataframe = self.cleanDataframeDatetime(dataframe)
                currentData = None

                fromDate = dataframe.index[0]
                toDate = dataframe.index[-1]

                try:

                    currentData = self.collection.item(itemName)
                except Exception as e:
                    logger.debug("symbol %s not found on database to load => return None %s" % (itemName, str(e)))

                if currentData is not None:
                    # check if append data or save normal
                    # currentDataDF = currentData.to_pandas()
                    # dateTimeindex = currentDataDF.index#.tz_localize('utc')
                    # fromDateDB = dateTimeindex[0]
                    # toDateDB = dateTimeindex[-1]
                    # if fromDate<fromDateDB or toDate>toDateDB:
                    logger.debug("updating to collection %s %s from %s to %s" % (
                        self.collectionName, itemName, str(fromDate), str(toDate)))
                    self.collection.append(itemName, dataframe, epochdate=epochDate)
                    output = True

                else:
                    logger.debug("new collection added %s from %s to %s" % (itemName, str(fromDate), str(toDate)))
                    self.collection.write(itemName, dataframe, metadata={'source': source}, epochdate=epochDate)
                    output = True
            except Exception as e:
                logger.error('some error saving %s : %s' % (itemName, str(e)))
                output = False
        return output

    def load_batch(self, itemNameList, startTime=None, endTime=None):
        # return dict of dataframes
        dictOut = {}
        for item in itemNameList:
            dictOut[item] = self.load(item, startTime, endTime)
        return dictOut

    def load(self, itemName, startTime=None, endTime=None):

        # Cache
        key = self.__getMD5_key__(itemName, startTime, endTime)
        if key in self.cacheDict.keys():
            return self.cacheDict[key]

        try:

            item = self.collection.item(itemName)
        except Exception as e:
            logger.error("symbol %s not found on database to load => return None %s" % (itemName, e))
            return None

        df = item.to_pandas()

        if startTime is not None:
            startTime = np.datetime64(startTime)
        if endTime is not None:
            endTime = np.datetime64(endTime)

        try:
            output = df
            mask = None

            if startTime is not None and endTime is not None:
                mask = (df.index >= startTime) & (df.index <= endTime)
                # output = df[startTime:endTime]
            elif startTime is not None:
                mask = (df.index >= startTime)
                # output = df[startTime:]
            elif endTime is not None:
                mask = (df.index <= endTime)
            if mask is not None:
                output = df.loc[mask]

            if len(output) == 0:
                output = None
            else:
                output = self.cleanDataframeDatetime(output)
                logger.debug("Successfully load from %s: %s a dataframe of %d rows %d columns" % (
                    self.collectionName, itemName, output.shape[0], output.shape[1]))
                self.cacheDict[key] = output  # Cache
        except Exception as e:
            logger.error('Error loading %s from %s return None :%s' % (itemName, item._path, str(e)))
            output = None

        return output

    def delete(self, itemName):
        # if instrument is None:
        #     # delete a complete collection
        #     self.store.delete_collection(self.collectionName)
        # else:
        # delete a item
        try:
            self.collection.delete_item(itemName)
            self.cacheDict = {}  # Cache delete everything....
        except Exception as e:
            logger.error("Error deleting collection %s:%s" % (itemName, e))
