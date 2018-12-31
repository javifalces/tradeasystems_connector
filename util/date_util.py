def convert_date(fromDate):
    import pandas as pd
    output = fromDate
    if fromDate is not None:

        # in case other type of date format
        if isinstance(fromDate, pd.Timestamp):
            # pd.Timestamp to datetime
            output = fromDate.to_pydatetime()

        # remove tzinfo
        output = output.replace(tzinfo=None)  # astimezone(tz=region_settings.timezone_setting)
    return output


def convertSerieClosestTimeIndex(serie, datetimeIndex, shift=0):
    closestSeries = (serie.index.searchsorted(datetimeIndex) - 1) + shift
    closestSeries = closestSeries[closestSeries < len(serie)]
    output = serie[closestSeries]
    return output
