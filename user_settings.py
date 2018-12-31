# database data path

from tradeasystems_connector.broker.email_connector import EmailConnector
from tradeasystems_connector.broker.gdax_connector import GdaxConnector
from tradeasystems_connector.broker.oanda_connector import OandaConnector
from tradeasystems_connector.fundamental_data.morningstar_fundamental_data import MorningStarFundamentalData
from tradeasystems_connector.fundamental_data.quandl_fundamental_data import QuandlFundamentalData
from tradeasystems_connector.historical_market_data.cryptocompare_historical_market_data import \
    CryptoCompareHistoricalMarketData
from tradeasystems_connector.historical_market_data.dukascopy_file_historical_market_data import \
    DukasCopyFileHistoricalMarketData
from tradeasystems_connector.historical_market_data.oanda_historical_market_data import OandaHistoricalMarketData
from tradeasystems_connector.historical_market_data.yahoo_historical_market_data import YahooHistoricalMarketData
from tradeasystems_connector.model.asset_type import AssetType

# database
# Dukascopy
dukascopy_source_folder = 'dukascopy_input'  # 'D:\\javif\\Coding\\Python\\AInvesting\\dukascopy_input'
ratio_source_folder = '\ratio_input'  # 'D:\\javif\\Coding\\Python\\AInvesting\\dukascopy_input'

# Quandl
quandl_token = 'xxxxxxxxxxxx'

# GDAX
gdax_token = "xxxxxxx"

# AlphaVantage
alphavantage_token = 'xxxxxxx'

# Oanda
oanda_environment = 'practice'  # live
oanda_id = 'xxxxx'
oanda_token = 'xxxxxxxxxxxxx-xxxxxx'

# IB
ib_host = '127.0.0.1'
ib_port = 7497
ib_client_id = 100
ib_account = 'xxxx'
# FXCM
fxcm_api = "xxxx"
fxcm_token = "xx"

# Bloomberg settings
bloomberg_port = 8194

# EMAIL
email_notify = 'asdasd@gmail.com'
email_address = 'asdasdas@gmail.com'
email_smtp_host = 'smtp.gmail.com'
email_smtp_port = 587
email_password = 'pass'

## asset_type to historical market data
asset_type_to_historical_market_data = {
    AssetType.index: YahooHistoricalMarketData,
    AssetType.crypto: CryptoCompareHistoricalMarketData,
    AssetType.es_equity: YahooHistoricalMarketData,
    AssetType.us_equity: YahooHistoricalMarketData,
    AssetType.etf: YahooHistoricalMarketData,
    AssetType.forex: OandaHistoricalMarketData,
    AssetType.dukascopy: DukasCopyFileHistoricalMarketData,

}

## asset_type to broker
asset_type_to_broker = {
    AssetType.crypto: GdaxConnector,
    AssetType.equity: EmailConnector,
    AssetType.etf: EmailConnector,
    AssetType.forex: OandaConnector,
    # AssetType.forex :

}
## asset_type_to_fundamental_data
asset_type_to_fundamental_data = {
    AssetType.index: QuandlFundamentalData,
    AssetType.crypto: QuandlFundamentalData,
    AssetType.es_equity: MorningStarFundamentalData,
    AssetType.us_equity: QuandlFundamentalData,
    AssetType.etf: QuandlFundamentalData,
    AssetType.forex: QuandlFundamentalData,
    AssetType.dukascopy: QuandlFundamentalData,

}

# to add to bundle to backtest
zipline_symbol_list = {
    'AAPL',
    'GOOGL'
}
