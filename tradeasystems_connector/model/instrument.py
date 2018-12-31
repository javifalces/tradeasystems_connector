from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.currency import Currency


class Instrument:
    symbol = None
    currency = None
    asset_type = None

    maturity = None
    tick_size = None
    pip_size = None
    min_tick = None

    def __init__(self, symbol, asset_type, broker=None, currency=Currency.eur, maturity=None):
        self.symbol = symbol
        self.broker = broker

        # set currency
        if asset_type == AssetType.us_equity:
            self.currency = Currency.usd
        elif asset_type == AssetType.es_equity:
            self.currency = Currency.eur
        else:
            self.currency = currency

        self.maturity = maturity
        self.asset_type = asset_type

    def __str__(self):
        return '%s_%s' % (self.symbol, self.currency)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, Instrument):
            equalitySymbol = (self.symbol == other.symbol)
            equalityCurrency = (self.currency == other.currency)
            return equalitySymbol and equalityCurrency
        return False


def __unstackInstrumentList__(instrumentList):
    instrumentExample = instrumentList[0]
    assetType = instrumentExample.asset_type
    currency = instrumentExample.currency
    instrumentStringList = [str(x) for x in instrumentList]
    return instrumentStringList, assetType, currency


def __stackInstrumentList__(instrumentStringList, assetType, currency):
    instrumentList = [
        Instrument(symbol=symbolString.split('_')[0], currency=symbolString.split('_')[1], asset_type=assetType) for
        symbolString in instrumentStringList]
    return instrumentList


ibex = Instrument('ibex', asset_type=AssetType.index, currency=Currency.eur)
sp500 = Instrument('gspc', asset_type=AssetType.index, currency=Currency.usd)
sp500_etf = Instrument('spy', asset_type=AssetType.etf, currency=Currency.usd)
vix = Instrument('VIX', currency='USD', asset_type=AssetType.index)
t_bond = Instrument('TNX', currency='USD', asset_type=AssetType.index)  # index for yahoo
bund = Instrument('BUNL', currency='EUR', asset_type=AssetType.bond)
dax = Instrument('GDAXI', currency='USD', asset_type=AssetType.index)
eonia = Instrument('C100.DE', currency='USD', asset_type=AssetType.us_equity)
eur_usd = Instrument('EUR', currency='USD', asset_type=AssetType.forex)
# eur_usd = Instrument('EURUSD=X', currency='USD', asset_type=AssetType.us_equity)
