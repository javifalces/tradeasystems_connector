from tradeasystems_connector.model.bar import Bar


class DataDictKeys:
    # each key has a dataframe of different instruments
    close = Bar.close
    open = Bar.open
    low = Bar.low
    high = Bar.high
    volume = Bar.volume

    index = Bar.index

    keys = [close, open, low, high, volume]
