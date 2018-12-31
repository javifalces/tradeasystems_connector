DATABASE_ENVIRONMENT_VARIABLE = 'TRADEA_DATABASE_PATH'
LOG_ENVIRONMENT_VARIABLE = 'TRADEA_LOG_PATH'
DUKASCOPY_INPUT_ENVIRONMENT_VARIABLE = 'TRADEA_DUKASCOPY_INPUT_PATH'
RATIO_INPUT_ENVIRONMENT_VARIABLE = 'TRADEA_RATIO_INPUT_PATH'
ENVIRONMENT = 'ENVIRONMENT'
import os
from pathlib import Path

DEFAULT = str(Path.home()) + os.sep


def checkMatplotlibEnvironment():
    if os.environ.get(ENVIRONMENT) is not None and \
            os.environ.get(ENVIRONMENT).upper() == 'JENKINS':
        print('Console System detected => use Agg matplotlib')
        import matplotlib

        matplotlib.use('Agg')
    else:
        import matplotlib
        import matplotlib.pyplot as plt
        #  Qt4Agg or TkAgg
        # matplotlib.use('Qt4Agg')
        plt.ion()


def getEnvironmentRegistry(key):
    import os
    output = os.environ.get(key)
    if output is not None and ';' in output:
        output = output.split(';')[0]
    return output


def getDatabasePath(userSettings):
    output = getEnvironmentRegistry(DATABASE_ENVIRONMENT_VARIABLE)

    if output is None:
        try:
            output = userSettings.database_path
        except:
            output = DEFAULT + 'database'

    if not os.path.isdir(output):
        os.mkdir(output)

    return output


def getTempPath(userSettings):
    output = getDatabasePath(userSettings) + os.sep + 'temp'

    if not os.path.isdir(output):
        os.mkdir(output)

    return output


def getLogPath(userSettings=None):
    output = getEnvironmentRegistry(LOG_ENVIRONMENT_VARIABLE)

    if output is None:
        try:
            output = userSettings.log_path
        except:
            output = DEFAULT + 'logs'

    if not os.path.isdir(output):
        print('creating log path in %s' % output)
        os.mkdir(output)
    return output


def getDukascopyInputPath(userSettings):
    output = getEnvironmentRegistry(DUKASCOPY_INPUT_ENVIRONMENT_VARIABLE)

    if output is None:
        try:
            output = userSettings.dukascopy_source_folder
        except:
            output = DEFAULT + 'dukascopy_input'

    if not os.path.isdir(output):
        os.mkdir(output)

    return output


def getRatioInputPath(userSettings):
    output = getEnvironmentRegistry(RATIO_INPUT_ENVIRONMENT_VARIABLE)

    if output is None:
        try:
            output = userSettings.ratio_source_folder
        except:
            output = DEFAULT + 'ratio_input'

    if not os.path.isdir(output):
        os.mkdir(output)

    return output
