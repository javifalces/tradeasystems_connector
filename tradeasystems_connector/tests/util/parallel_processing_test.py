import time
import unittest

from tradeasystems_connector.tests import user_settings_tests
from tradeasystems_connector.util.parallel_processing import mpPandasObj


def readDictionary():
    from tradeasystems_connector.util.persist_util import load_from_file
    return load_from_file('dictOfMatrix.pickle')


def processingDataDict_666(data, symbolList):
    output = data.copy()
    for key in data.keys():
        # output[key] = data[key].copy()
        for symbol in output[key].columns:
            if symbol in symbolList:
                output[key][symbol] = 666
            else:
                output[key][symbol] = 0
    return output


class ParallelProcessingUtilTest(unittest.TestCase):
    user_settings = user_settings_tests

    @unittest.skip
    def test_compare_dict_mpPandasObj(self):
        data = readDictionary()
        symbolList = list(data['close'].columns[:10])
        start = time.time()
        serialized_dict = processingDataDict_666(data, symbolList)
        print('Serialized Processing took %d seconds' % (time.time() - start))
        start = time.time()
        parallel_dict = mpPandasObj(func=processingDataDict_666, pdObj=('symbolList', symbolList),
                                    mpBatches=1, linMols=True, isVerticalParallel=True, numThreads=4,
                                    data=data
                                    )
        print('Parallel Processing took %d seconds' % (time.time() - start))
        self.assertIsNotNone(serialized_dict)
        self.assertIsNotNone(parallel_dict)
        self.assertEqual(len(parallel_dict.keys()), len(serialized_dict.keys()))
        for key in parallel_dict.keys():
            for symbol in parallel_dict[key].columns:
                originalData = data[key][symbol]
                data_serial = serialized_dict[key][symbol]
                data_parallel = parallel_dict[key][symbol]
                self.assertEqual(len(originalData), len(data_serial))
                self.assertEqual(len(originalData), len(data_parallel))
                for row in originalData.index:
                    self.assertNotEquals(666, originalData[row])
                    if symbol in symbolList:
                        self.assertEqual(666, data_serial[row])
                        self.assertEqual(666, data_parallel[row])
                    else:
                        self.assertEqual(0, data_serial[row])
                        self.assertEqual(0, data_parallel[row])
