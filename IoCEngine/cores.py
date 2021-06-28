import multiprocessing
from functools import partial

import numpy as np
import pandas as pd

from IoCEngine.commons import count_down, get_logger
from IoCEngine.config.pilot import mpcores

mdjlogger = get_logger('jarvis')


# def ppls(f, df, re=False):
#     try:
#         processes = multiprocessing.cpu_count() - 1
#         chunk_size = int(df.shape[0] / processes)
#         chunks = [df.ix[df.index[i:i + chunk_size]] for i in range(0, df.shape[0], chunk_size)]
#         pool = multiprocessing.Pool(processes=processes)
#         res = pool.map(partial(f, chunks, ))
#         if re:
#             for i in range(len(res)):
#                 df.iloc[res[i].index] = res[i]
#         return df
#     except Exception as e:
#         mdjlogger.error(e)


def ppns(f, df, listargs, re=False):
    try:
        loadedbatch = listargs[0] if isinstance(listargs, list) else listargs
        if not df.empty:
            # chunk_size = int(df.shape[0] / processes)
            df_shape = df.shape
            processes = mpcores if df_shape[0] > 5731 else 1
            if df_shape[0] <= 5731:
                mdjlogger.info('not spurning any new process for {} | {} . ..'.format(loadedbatch, df_shape))
                return f(listargs, df)

            parts = processes
            mdjlogger.info("should split this {} {} into {}".format(loadedbatch, df_shape, parts))
            count_down(None, 10)
            # chunks = [df.iloc[df.index[i:i + chunk_size]] for i in range(0, df.shape[0], chunk_size)]
            chunks = np.array_split(df, parts)
            pool = multiprocessing.Pool(processes=processes)
            #
            # pool.starmap(partial(f, chunks, data_tpl=args[0][0], cols=args[0][1], data_store=args[0][2]))
            if re:
                # mgr = multiprocessing.Manager()

                if listargs:
                    func = partial(f, listargs)
                    df = pd.concat(pool.map(func, chunks))
                else:
                    df = pd.concat(pool.map(f, chunks))
            else:
                func = partial(f, listargs)
                pool.map(func, chunks)
            pool.close()
            pool.join()
            pool.terminate()
            if re:
                mdjlogger.info("returning {} from {} split parts".format(df.shape, parts))
                count_down(None, 10)
                return df
                #
                # res = pool.map(func, chunks)

                # for i in range(len(res)):
                #     df.ix[res[i].index] = res[i]
                #
                # return df
    except Exception as e:
        mdjlogger.error(e)
