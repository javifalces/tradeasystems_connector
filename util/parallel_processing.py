import sys
import time
from collections import abc

import numpy as np


# linear partitions
def linParts(numAtoms, numThreads):
    # partition of atoms with a single loop
    parts = np.linspace(0, numAtoms, min(numThreads, numAtoms) + 1)
    parts = np.ceil(parts).astype(int)
    return parts


def nestedParts(numAtoms, numThreads, upperTriang=False):
    # partition of atoms with an inner loop
    parts, numThreads_ = [0], min(numThreads, numAtoms)
    for num in range(numThreads_):
        part = 1 + 4 * (parts[-1] ** 2 + parts[-1] + numAtoms * (numAtoms + 1.) / numThreads_)
        part = (-1 + part ** .5) / 2.
        parts.append(part)
    parts = np.round(parts).astype(int)
    if upperTriang:  # the first rows are heaviest
        parts = np.cumsum(np.diff(parts)[::-1])
        parts = np.append(np.array([0]), parts)
    return parts


import pathos.multiprocessing as mp  # use dill instead of pickle
import datetime as dt


def reportProgress(jobNum, numJobs, time0, task):
    # Report progress as asynch jobs are completed
    msg = [float(jobNum) / numJobs, (time.time() - time0) / 60.]
    msg.append(msg[1] * (1 / msg[0] - 1))
    timeStamp = str(dt.datetime.fromtimestamp(time.time()))
    msg = timeStamp + ' ' + str(round(msg[0] * 100, 2)) + '% ' + task + ' done after ' + \
          str(round(msg[1], 2)) + ' minutes. Remaining ' + str(round(msg[2], 2)) + ' minutes.'
    if jobNum < numJobs:
        sys.stderr.write(msg + '\r')
    else:
        sys.stderr.write(msg + '\n')
    return


def processJobs(jobs, task=None, numThreads=24):
    # Run in parallel.
    # jobs must contain a 'func' callback, for expandCall
    if task is None: task = jobs[0]['func'].__name__
    pool = mp.Pool(processes=numThreads, maxtasksperchild=1000)
    outputs, out, time0 = pool.imap_unordered(expandCall, jobs), [], time.time()
    # Process asyn output, report progress
    for i, out_ in enumerate(outputs, 1):
        out.append(out_)
        reportProgress(i, len(jobs), time0, task)
    pool.close();
    pool.join()  # this is needed to prevent memory leaks
    return out


def expandCall(kargs):
    # Expand the arguments of a callback function, kargs['func']
    func = kargs['func']
    del kargs['func']
    out = func(**kargs)
    return out


def processJobs_(jobs):
    # Run jobs sequentially, for debugging
    out = []
    for job in jobs:
        out_ = expandCall(job)
        out.append(out_)
    return out


def mpPandasObj(func, pdObj, numThreads=24, mpBatches=1, linMols=True, isVerticalParallel=False, **kargs):
    '''
    Parallelize jobs, return a dataframe or series
    + func: function to be parallelized. Returns a DataFrame
    + pdObj[0]: Name of argument used to pass the molecule
    + pdObj[1]: List of atoms that will be grouped into molecules
    + numthreads = num processes that will be used in parallel
    + mpBatchs = number of parallel batches,jobs per process
    + linMols or nestedMols => if first mol more heavy thatn the rest
    + isVerticalParallel => true if allign in the end vertically (columns), not horizontal
    + kwds: any other argument needed by func

    Example: df1=mpPandasObj(func,('molecule',df0.index),24,**kwds)
    '''
    import pandas as pd
    # if linMols:parts=linParts(len(argList[1]),numThreads*mpBatches)
    # else:parts=nestedParts(len(argList[1]),numThreads*mpBatches)
    if linMols:
        parts = linParts(len(pdObj[1]), numThreads * mpBatches)
    else:
        parts = nestedParts(len(pdObj[1]), numThreads * mpBatches)

    jobs = []
    for i in range(1, len(parts)):
        job = {pdObj[0]: pdObj[1][parts[i - 1]:parts[i]], 'func': func}
        job.update(kargs)
        jobs.append(job)
    if numThreads == 1:
        out = processJobs_(jobs)
    else:
        out = processJobs(jobs, numThreads=numThreads)
    if isinstance(out[0], pd.DataFrame):
        df0 = pd.DataFrame()

    elif isinstance(out[0], pd.Series):
        df0 = pd.Series()

    # Add to multiprocessing vectorized if is a dict!
    elif isinstance(out[0], abc.Mapping):
        dictOutput = {}
        for i in out:
            for keyOfDict in i:
                dataframe = i[keyOfDict]
                if keyOfDict not in dictOutput:
                    dictOutput[keyOfDict] = dataframe
                else:
                    if isinstance(dictOutput[keyOfDict], pd.DataFrame):
                        if not isVerticalParallel:
                            dictOutput[keyOfDict] = dictOutput[keyOfDict].append(dataframe)
                            dictOutput[keyOfDict] = dictOutput[keyOfDict].sort_index()
                        else:
                            joinDF(dictOutput[keyOfDict], dataframe)
                    else:
                        listValues = dataframe
                        if listValues is not None and len(listValues) > 0:
                            dictOutput[keyOfDict].append(listValues)

            # for keyOfDict in dict:
            #     dataframe = dict[keyOfDict]
            #     if keyOfDict not in dictOutput:
            #         dictOutput[keyOfDict] = dataframe
            #     else:
            #         if isinstance(dictOutput[keyOfDict], pd.DataFrame):
            #             columnSeries = dictOutput[keyOfDict].dropna(axis=1)
            #             column = columnSeries.columns[0]
            #             dictOutput[keyOfDict][column] = dataframe[column]
            #         else:
            #             #is a list
            #             if dataframe is not None and len(dataframe)>0:
            #                 dictOutput[keyOfDict].append(dataframe)
        return dictOutput
    else:
        return out

    # If is a dataframe processing/series
    for i in out:
        if isVerticalParallel:
            df0 = joinDF(df0, i)
        else:
            df0 = df0.append(i)
    df0 = df0.sort_index()
    return df0


def joinDF(df0, dataframeIn):
    if len(df0) == 0:
        df0 = dataframeIn
    else:
        i_clean = dataframeIn.replace([np.inf, -np.inf], np.nan)
        i_clean = i_clean.fillna(0)
        i_clean = dataframeIn[dataframeIn.columns[(i_clean.sum().astype(int) != 0) == True]]
        df0[i_clean.columns] = dataframeIn[i_clean.columns]
    return df0
