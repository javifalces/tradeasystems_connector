# decorator cache in memory


def memoize(function):
    memo = {}

    def wrapper(*args):
        if args in memo:
            return memo[args]
        else:
            rv = function(*args)
            memo[args] = rv
            return rv

    return wrapper


#
# # decorator cache in memory, limit
# def memoize(function, limit=None):
#     import pickle
#     if isinstance(function, int):
#         def memoize_wrapper(f):
#             return memoize(f, function)
#
#         return memoize_wrapper
#
#     dict = {}
#     list = []
#
#     def memoize_wrapper(*args, **kwargs):
#         key = pickle.dumps((args, kwargs))
#         try:
#             list.append(list.pop(list.index(key)))
#         except ValueError:
#             dict[key] = function(*args, **kwargs)
#             list.append(key)
#             if limit is not None and len(list) > limit:
#                 del dict[list.pop(0)]
#
#         return dict[key]
#
#     memoize_wrapper._memoize_dict = dict
#     memoize_wrapper._memoize_list = list
#     memoize_wrapper._memoize_limit = limit
#     memoize_wrapper._memoize_origfunc = function
#     memoize_wrapper.func_name = function.func_name
#     return memoize_wrapper


# decorator cache in hard disk
import os

import joblib

from tradeasystems_connector.util.configuration_keys_util import getEnvironmentRegistry, DATABASE_ENVIRONMENT_VARIABLE

DEFAULT_PATH = getEnvironmentRegistry(DATABASE_ENVIRONMENT_VARIABLE) + os.sep + 'temp'


def memoizeHD(function, path=DEFAULT_PATH):
    memo = {}
    if not os.path.exists(path):
        raise FileExistsError('%s not exist to cache it!' % path)
    else:
        cacher = joblib.Memory(path)

    def wrapper(*args):
        if args in memo:
            # try in memory
            return memo[args]
        else:
            # try in HD
            functionTemp = cacher.cache(func=function)
            rv = functionTemp(*args)
            memo[args] = rv
            return rv

    return wrapper
