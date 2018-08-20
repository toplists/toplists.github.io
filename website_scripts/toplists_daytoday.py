#!/usr/bin/env python3

import zipfile
import pandas as pd
# import pandas as pd
import json
import numpy as np
import datetime
from datetime import timedelta
import sys
import time
import os
import lzma
import glob
VERSION = "20180528_1908_CEST"
LIMIT=1000000
GLOBALPATH="/srv/public/archive/"


def read_csv(filehandle, names=None, limit=None, name=None, dtypes=None):
    try:
        data = pd.read_csv(filehandle, names=names, nrows=LIMIT, dtype=dtypes, keep_default_na=False)
    except UnicodeDecodeError as exc:
        raise Exception("Failed to read csv file %s" % filehandle) from exc
    if len(data) < LIMIT:
        print("File {} only has {} lines!".format(name, len(data)))
        return [False]
    if len(data[pd.isnull(data).any(axis=1)]) > 0:
        print("File {} has nans: {}!".format(name, data[pd.isnull(data).any(axis=1)]))
        return [False]
    return data


def read_alexa(date):
    return set(read_alexa_aslist(date))


def find_first_fn(name):
    # print(name)
    x = glob.glob(name)
    if len(x) == 0:
        return False
    else:
        return x[0]


def read_alexa_aslist(date):
    name = GLOBALPATH + "alexa/alexa-top1m-" + date + "*.csv.xz"
    name = find_first_fn(name)
    alexa_columns = ["rank", "domain"]
    dtypes = {"rank": np.int32, "domain": str}
    with lzma.open(name, mode='rt') as F:
        data = read_csv(F, names=alexa_columns, limit=LIMIT, name=name, dtypes=dtypes)
    if len(data) < LIMIT:
        print("File {} only has {} lines!".format(name, len(data)))
        raise ValueError('Too few lines read!')
    return data.domain.values



def read_umbrella_aslist(date):
    name = GLOBALPATH +  "umbrella/cisco-umbrella-top1m-" + date + "*.csv.xz"
    name = find_first_fn(name)
    # zipname = "top-1m.csv"
    alexa_columns = ["rank", "domain"]
    dtypes = {"rank": np.int32, "domain": str}
    with lzma.open(name, mode='rt') as F:
        data = read_csv(F, names=alexa_columns, name=name, dtypes=dtypes, limit=LIMIT)
    if len(data) < LIMIT:
        print("File {} only has {} lines!".format(name, len(data)))
        raise ValueError('Too few lines read!')
    return data.domain.values


def read_majestic_aslist(date):
    name = GLOBALPATH + "majestic/majestic_million_" + date + "*.csv.xz"
    name = find_first_fn(name)
    with lzma.open(name, mode='rt') as F:
        data = pd.read_csv(F, encoding="utf-8", usecols=["Domain"], nrows=LIMIT)
    if len(data) < LIMIT:
        print("File {} only has {} lines!".format(name, len(data)))
        raise ValueError('Too few lines read!')
    return data.Domain.values


class MyEncoder(json.JSONEncoder):
    # very annoyingly, json.dumps fails when deadling with np objects
    def default(self, obj):
        if isinstance(obj, np.integer):
            return str(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return sorted(obj.tolist())
        elif isinstance(obj, set):
            return sorted(list(obj))
        else:
            return super(MyEncoder, self).default(obj)


def do(dt, filemode="all"):
    dto = datetime.datetime.strptime(dt, '%Y-%m-%d')
    dtn = dto + timedelta(1)
    dtn = dtn.strftime("%Y-%m-%d")
    if filemode != "majestic" and filemode != "umbrella" and filemode!= "all":
        filemode = "alexa"
    stats = {}  # it is critical to make stats empty before each run
    stats["date"] = dt
    stats["date_next"] = dtn
    stats["VERSION"] = VERSION
    #stats["STATSF"] = "./analysis_daytoday/{}_{}.json".format(filemode, dt)
    stats["STATSF"] = "./analysis_daytoday/{}.json".format(dt)
    if os.path.isfile(stats["STATSF"]):
        with open(stats["STATSF"], 'r') as F:
            if json.load(F)["VERSION"] == stats["VERSION"]:
                print("Already done: {}".format(dt))
                return
    print("Working on date: {}".format(dt))
    # majestic, umbrella = True, True
    filemodes = []
    if filemode != "all":
        filemodes = [filemode]
    elif filemode == "all":
        filemodes = ["alexa", "umbrella", "majestic"]
    else:
        print("Weird filemode! exiting -- {}".format(filemode))
    for filemode in filemodes:
        try:
            if filemode == "umbrella":
                a = read_umbrella_aslist(dt)
                b = read_umbrella_aslist(dtn)
            elif filemode == "majestic":
                a = read_majestic_aslist(dt)
                b = read_majestic_aslist(dtn)
            else:
                a = read_alexa_aslist(dt)
                b = read_alexa_aslist(dtn)
                filemode = "alexa"
        except Exception as e:
            print("Exception when loading {} files for dt {}: {}, skipping {}".format(filemode, dt, e, filemode))
            continue

        sa = set([a[0]])
        sb = set([b[0]])
        x = []
        # d = {}
        steps = [0, 10,100,1000,10000, 100000, 1000000]
        for i in range(len(steps)-1):
            #print(len(steps),i, steps[i], steps[i+1])
            #continue
            sa.update(set(a[steps[i]:steps[i+1]]))  # = (sa | atmp)
            sb.update(set(b[steps[i]:steps[i+1]]))  # = (sa | atmp)
            x.append((steps[i+1], len(sb - sa)))
        for i in x:
            k, v = i[0], i[1]
            # stats["timedeltas_" + filemode + "_" + k] = v
            stats["timedeltas_{}_{}".format(filemode, k)] = v

    with open(stats["STATSF"], 'w') as F:
        json.dump(stats, F, cls=MyEncoder, sort_keys=True)
    print("{}-{} done after {:.2f} seconds".format(filemode, dt, time.time() - start_time))


def main():
    if len(sys.argv) > 1:
        print("Working on date: {}".format(sys.argv[1]))
        if len(sys.argv) > 2:
            do(sys.argv[1], filemode=sys.argv[2])
        else:
            do(sys.argv[1], filemode="all")
    else:
        for i in range(1,10):
            dt = datetime.date.today() - timedelta(i)
            dt = dt.strftime("%Y-%m-%d")
            # print("Working on date: {}".format(dt))
            do(dt, "all")


if __name__ == "__main__":
    start_time = time.time()
    main()
