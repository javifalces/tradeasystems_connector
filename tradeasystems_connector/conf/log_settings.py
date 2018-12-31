import datetime
import logging
import os

from tradeasystems_connector.conf import region_settings
from tradeasystems_connector.util.configuration_keys_util import getLogPath

log_path = getLogPath()  #

framework_name = 'tradeasystems_connector'
level = logging.DEBUG
formatLog = ' %(asctime)s -[%(filename)s:%(lineno)s - %(funcName)20s() ] %(levelname)s - %(message)s'

# %%
logger = logging.getLogger(framework_name)
logger.setLevel(level)

# create a file handler
log_name = '%s_%s.log' % (framework_name, datetime.datetime.today().strftime(region_settings.dateformat_file))
handler = logging.FileHandler(log_path + os.sep + log_name)
handler.setLevel(logging.DEBUG)

# create a logging format
formatter = logging.Formatter(formatLog)
handler.setFormatter(formatter)
# To print on screen
import sys

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(handler)
logger.addHandler(ch)

# logger.info('Started Log')
