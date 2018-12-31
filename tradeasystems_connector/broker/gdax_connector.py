import gdax

from tradeasystems_connector.broker.broker_connector import BrokerConnector
from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.model.account_balance import AccountBalance
from tradeasystems_connector.model.side import Side


class GdaxConnector(BrokerConnector):
    gdax = None
    user_settings = None

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.gdax = gdax.AuthenticatedClient(self.user_settings.gdax_token)
        pass

    def sendOrder(self, order):
        productId = str.upper('%s-%s' % (order.instrument.symbol, order.instrument.currency))

        if order.side == Side.buy:
            answer = self.gdax.buy(price=order.price, size=order.volume, product_id=productId)
        if order.side == Side.sell:
            answer = self.gdax.sell(price=order.price, size=order.volume, product_id=productId)

        # TODO
        # idOrder = answer.
        # order.unique_id = idOrder

        return order

    def cancelOrder(self, order):
        if order.unique_id is not None:
            self.gdax.cancel_order(order.unique_id)
        else:
            logger.error("Cant cancel order without id! %s" % order.instrument.symbol)

    def getAccountBalance(self, broker):
        accountList = self.gdax.get_accounts()
        for account in accountList:
            print(account)
        accountBalance = AccountBalance()

        raise NotImplementedError
