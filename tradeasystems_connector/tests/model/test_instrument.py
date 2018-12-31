import unittest

from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import __unstackInstrumentList__, __stackInstrumentList__, Instrument


class InstrumentTest(unittest.TestCase):
    instrumentList = [
        Instrument(symbol='ABC', asset_type=AssetType.us_equity, currency=Currency.usd),
        Instrument(symbol='ACN', asset_type=AssetType.us_equity, currency=Currency.usd),
        Instrument(symbol='AAPL', asset_type=AssetType.us_equity, currency=Currency.usd),
        Instrument(symbol='GOOGL', asset_type=AssetType.us_equity, currency=Currency.usd),
        Instrument(symbol='MSFT', asset_type=AssetType.us_equity, currency=Currency.usd),

    ]

    def test_checkUnstackStackInstrumentList(self):
        symbolList, assetType, currency = __unstackInstrumentList__(self.instrumentList)
        self.assertIsNotNone(symbolList)
        self.assertIsNotNone(assetType)
        self.assertIsNotNone(currency)

        self.assertEqual(len(symbolList), len(self.instrumentList))

        instrumentListBack = __stackInstrumentList__(symbolList, assetType, currency)
        self.assertIsNotNone(instrumentListBack)
        self.assertEqual(len(instrumentListBack), len(self.instrumentList))
