from tradeasystems_connector.broker.email_connector import EmailConnector
from tradeasystems_connector.broker.gdax_connector import GdaxConnector
from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.fundamental_data.quandl_fundamental_data import QuandlFundamentalData

from tradeasystems_connector.historical_market_data.cryptocompare_historical_market_data import \
    CryptoCompareHistoricalMarketData
from tradeasystems_connector.historical_market_data.yahoo_historical_market_data import YahooHistoricalMarketData
from tradeasystems_connector.model.asset_type import AssetType


class RouterProvider:
    user_settings = None

    def __init__(self, user_settings):
        self.user_settings = user_settings
        pass

    def getHistoricalMarketDataProvider(self, instrument):
        asset_requested = instrument.asset_type
        try:
            provider = self.user_settings.asset_type_to_historical_market_data[asset_requested]
        except:
            logger.info(
                'No provided historical market in user_settings as dict asset_type_to_historical_market_data=> default')
            if asset_requested == AssetType.crypto:
                provider = CryptoCompareHistoricalMarketData
            else:
                provider = YahooHistoricalMarketData

        return provider

    def getFundamentalDataProvider(self, ratio, instrument):
        asset_requested = instrument.asset_type
        try:
            provider = self.user_settings.asset_type_to_fundamental_data[asset_requested]
        except:
            logger.info(
                'No provided fundamental data provider in user_settings as dict asset_type_to_fundamental_data=> default')
            provider = QuandlFundamentalData

        return provider

    def getBroker(self, instrument):
        asset_requested = instrument.asset_type
        try:
            provider = self.user_settings.asset_type_to_broker[asset_requested]

        except:
            logger.info(
                'No provided broker in user_settings as dict asset_type_to_broker=> default')
            if asset_requested == AssetType.crypto:
                provider = GdaxConnector
            else:
                provider = EmailConnector

        return provider
