class BrokerConnector:

    def __init__(self, user_settings):
        pass

    def sendOrder(self, order):
        raise NotImplementedError

    def cancelOrder(self, order):
        raise NotImplementedError

    def getAccountBalance(self, broker):
        raise NotImplementedError
