from tradeasystems_connector.service.router_provider import RouterProvider


class TradingService:
    user_settings = None
    routerProvider = None

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.routerProvider = RouterProvider(self.user_settings)

    def subscribeTickData(self, instrument):
        # instrument has the broker to subscribe
        raise NotImplementedError
        pass

    def sendOrder(self, order):
        broker = self.routerProvider.getBroker(order.instrument)
        broker = broker(self.user_settings)
        broker.sendOrder(order)

    def cancelOrder(self, order):
        broker = self.routerProvider.getBroker(order.instrument)
        broker = broker(self.user_settings)
        broker.cancelOrder(order)

    def getAccountBalance(self, broker):
        # broker = self.routerProvider.getBroker(broker)
        broker = broker(self.user_settings)
        broker.getAccountBalance()
