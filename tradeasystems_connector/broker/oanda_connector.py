import oandapy

from tradeasystems_connector.broker.broker_connector import BrokerConnector
from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.model.account_balance import AccountBalance
from tradeasystems_connector.model.order_type import OrderType
from tradeasystems_connector.model.side import Side
from tradeasystems_connector.model.time_in_force import TimeInForce


class OandaConnector(BrokerConnector):
    oanda = None
    user_settings = None
    account_id = None
    dict_side = {
        Side.sell: 'sell',
        Side.buy: 'buy'
    }
    dict_order_type = \
        {
            OrderType.limit: 'limit',
            OrderType.market: 'market'
        }

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.oanda = oandapy.API(environment=self.user_settings.oanda_environment,
                                 access_token=self.user_settings.oanda_token)
        self.account_id = self.user_settings.oanda_id
        pass

    def calculateExpiration(self, time_in_force):
        import datetime
        if time_in_force == TimeInForce.day:
            trade_expire = datetime.utcnow() + datetime.timedelta(days=1)
            trade_expire = trade_expire.isoformat("T") + "Z"
            return trade_expire
        elif time_in_force == TimeInForce.gtc:
            # trade_expire =  datetime.utcnow() + datetime.timedelta(days=1000)
            return None

    def sendOrder(self, order):
        productId = str.upper('%s_%s' % (order.instrument.symbol, order.instrument.currency))
        expiration = self.calculateExpiration(order.time_in_force)
        response = self.oanda.create_order(self.account_id,
                                           instrument=productId,
                                           units=order.volume,
                                           side=self.dict_side[order.side],
                                           type=self.dict_order_type[order.order_type],
                                           price=order.price,
                                           expiry=expiration
                                           )
        # TODO
        # idOrder = response.
        # order.unique_id = idOrder

        return order

    def cancelOrder(self, order):
        if order.unique_id is not None:
            response = self.oanda.close_order(self.account_id, order.unique_id)
        else:
            logger.error("Cant cancel order without id! %s" % order.instrument.symbol)

    def getAccountBalance(self, broker):
        accountResponse = self.oanda.get_account(self.account_id)

        accountBalance = float(accountResponse['balance'])

        output = AccountBalance()
        output.realized_pnl = accountBalance

        return output
