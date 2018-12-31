from tradeasystems_connector.model.order_type import OrderType
from tradeasystems_connector.model.time_in_force import TimeInForce


class Order:
    volume = None
    price = None
    side = None
    order_type = None
    instrument = None
    time_in_force = None

    unique_id = None

    def __init__(self, instrument, volume, side, price=None, order_type=OrderType.market,
                 time_in_force=TimeInForce.day):
        self.instrument = instrument
        self.volume = volume
        self.side = side
        self.price = price
        self.order_type = order_type
        self.time_in_force = time_in_force
        self.unique_id = None
