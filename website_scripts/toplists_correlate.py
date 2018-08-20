#!/usr/bin/env python3

import zipfile
import pandas
import pandas as pd
import json
import numpy as np
import os
import glob
import lzma
import pickle
import subprocess
BIGVERSION = "3"
VERSION = "20180529_1715_CEST"
LIMIT=1000000
LIMITSTR="1M"
GLOBALPATH="/srv/public/archive/"
PSL_GITHASH=""


#def get_git_revision_hash():
    #return subprocess.check_output(['git', 'rev-parse', 'HEAD'])
    #return subprocess.check_output(['cd /srv/psl/ && git rev-parse HEAD'], shell=True)

#def get_git_revision_short_hash():
#    return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'])


def read_csv(filehandle, names=None, limit=None, name=None, dtypes=None):
    try:
        data = pandas.read_csv(filehandle, names=names, nrows=LIMIT, dtype=dtypes, keep_default_na=False)
    except UnicodeDecodeError as exc:
        raise Exception("Failed to read csv file %s" % filehandle) from exc
    if len(data) < LIMIT:
        print("File {} only has {} lines!".format(name, len(data)))
        sys.exit(1)
    if len(data[pd.isnull(data).any(axis=1)]) > 0:
        print("File {} has nans: {}!".format(name, data[pd.isnull(data).any(axis=1)]))
        sys.exit(1)
    return data


def find_first_fn(name):
    # print(name)
    x = glob.glob(name)
    if len(x) == 0:
        return False
    else:
        return x[0]


def read_alexa(date):
    name = GLOBALPATH + "alexa/alexa-top1m-" + date + "*.csv.xz"
    name = find_first_fn(name)
    # print(name)
    # zipname = "top-1m.csv"
    alexa_columns = ["rank", "domain"]
    dtypes = {"rank": np.int32, "domain": str}
    with lzma.open(name, mode='rt') as F:
        data = read_csv(F, names=alexa_columns, limit=LIMIT, name=name, dtypes=dtypes)
    if len(data) < LIMIT:
        print("File {} only has {} lines!".format(name, len(data)))
        sys.exit(1)
    return set(data.domain.values)


def read_umbrella(date):
    name = GLOBALPATH +  "umbrella/cisco-umbrella-top1m-" + date + "*.csv.xz"
    name = find_first_fn(name)
    # zipname = "top-1m.csv"
    alexa_columns = ["rank", "domain"]
    dtypes = {"rank": np.int32, "domain": str}
    with lzma.open(name, mode='rt') as F:
        data = read_csv(F, names=alexa_columns, name=name, dtypes=dtypes, limit=LIMIT)
    if len(data) < LIMIT:
        print("File {} only has {} lines!".format(name, len(data)))
        sys.exit(1)
    return set(data.domain.values)


def read_majestic(date):
    name = GLOBALPATH + "majestic/majestic_million_" + date + "*.csv.xz"
    name = find_first_fn(name)
    with lzma.open(name, mode='rt') as F:
        data = pandas.read_csv(F, encoding="utf-8", usecols=["Domain"], nrows=LIMIT)
    if len(data) < LIMIT:
        print("File {} only has {} lines!".format(name, len(data)))
        sys.exit(1)
    return set(data.Domain.values)


psl = set()
import time
start_time = time.time()
import subprocess

def read_tlds():
    tldf = '/srv/psl/public_suffix_list.dat.sortu.lower'
    sha512sum = "6db75f78696d0031c4ca712612b15a32650aedf99be7e89599910ad3e262999fedcf5abfbf853340b5ddae3f9836f28a275f97241d628152a452b173af7b9116"
    shasum = subprocess.run(['sha512sum', tldf], stdout=subprocess.PIPE).stdout.decode('ascii').split()[0]
    if shasum != sha512sum:
        print("ERROR! Sha512-sum mismatch for tld file!")
        sys.exit(1)
    with open(tldf, "r") as F:
            x = F.readlines()
            s = set()
            for i in x:
                s.add(i.rstrip())
    return s


def read_psl():
    import io
    global psl
    # only load PSL once per run
    if len(psl) > 10:
        return
    global PSL_GITHASH
    PSL_GITHASH = subprocess.check_output(['cd /srv/psl/ && git rev-parse HEAD'], shell=True).decode('ascii').rstrip()
    # cat /srv/psl/public_suffix_list.dat | sort -u | grep -v '^//' | tr '[:upper:]' '[:lower:]'  > /srv/psl/public_suffix_list.dat.sortu.lower
    pslf = "/srv/psl/public_suffix_list.dat" # .sortu.lower"
    size = os.path.getsize(pslf)
    try:
        [sizeold, psl] = pickle.load(open(pslf + ".pickle", "rb"))
        if sizeold == size:
            print("Read {} PSL domains from pickle after {:.2f} seconds.".format(len(psl), time.time() - start_time))
            return
    except FileNotFoundError:
        pass
    with io.open(pslf, 'r', encoding='utf8') as FILE:
        for line in FILE:
            li = line.strip()
            # skip comments lines that start with / and empty lines starting with " "
            if li.startswith("/") or li.startswith(" ") or len(li) == 0:
                continue
            psl.add(li)
    # stats["psl_len"] = len(psl)
    pickle.dump([size, psl], open(pslf + ".pickle", "wb"))
    print("Read {} PSL domains after {:.2f} seconds.".format(len(psl), time.time() - start_time))


def find_basedomain(x, psl, level):  # finds the longest PSL match +1
    if ".".join(x[-level:]) in psl:
        if level == len(x):
            return ".".join(x[-level:])
        try:
            return find_basedomain(x, psl, level + 1)
        except RecursionError:
            print("RecursionError for domain {} at level {}".format(x, level))
    else:
        return ".".join(x[-level:])


def eval_list4psl(l, psl):
    retl = []
    for i in list(l):
        j = i.split('://')[-1]
        j = j.split('/')[0]
        try:
            bd = find_basedomain(j.split("."), psl, 1)  # basedomain is PSL +1
        except:
            # print("Error: {} in {}".format(i, l))
            print("Error domain: {}".format(i))
            sys.exit(1)
        psld = ".".join(bd.split(".")[1:])  # public suffix domain
        depth = len(j.split(".")) - len(psld.split("."))
        sld = bd.split(".")[0]
        tld = bd.split(".")[-1]
        retl.append((i, depth, bd, psld, sld, tld))
        # if depth > 1:
        #   # print(i, bd, depth)
        #  ret.append(i) # [i, bd, depth])
    return pd.DataFrame.from_records(retl, index=None, exclude=None,
        columns=["entry", "subdomain_depth", "basedomain", "psld", "sld", "tld"],
        coerce_float=False, nrows=None)


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


def do(dt):
    stats = {} # it is critical to make stats empty before each run
    stats["date"] = dt
    stats["STATSF"] = "./analysis_correlation/{}_v{}_{}.json".format(dt, BIGVERSION, LIMITSTR)
    stats["VERSION"] = VERSION
    if os.path.isfile(stats["STATSF"]):
        with open(stats["STATSF"], 'r') as F:
            if json.load(F)["VERSION"] == stats["VERSION"]:
                print("Already done: {}".format(dt))
                return

    print("Working on date: {}".format(dt))
    majestic, umbrella = True, True
    try:
        alexaset = read_alexa(dt)
    except Exception as e:
        print("Exception when loading Alexa files for dt {}: {}, skipping".format(dt, e))
        return
    try:
        majesticset = read_majestic(dt)

    except Exception as e:
        majestic = False
        print("Majestic failed: {}".format(e))

    try:
        umbrellaset = read_umbrella(dt)
    except Exception as e:
        print("Umbrella failed: {}".format(e))
        umbrella = False

    stats["alexa"] = True
    stats["majestic"] = majestic
    stats["umbrella"] = umbrella

    # Correlate Raw Domains

    if majestic:
        stats["corr_raw_alexa_majestic"] = len(alexaset & majesticset)
    if umbrella:
        stats["corr_raw_alexa_umbrella"] = len(alexaset & umbrellaset)
    if majestic and umbrella:
        stats["corr_raw_umbrella_majestic"] = len(umbrellaset & majesticset)
        stats["corr_raw_alexa_umbrella_majestic"] = len(umbrellaset & majesticset & alexaset)

    # load PSL late to fail early if eg files are missing
    read_psl()
    stats["PSL_GITHASH"] = PSL_GITHASH
    validtlds = read_tlds()
    L = "alexa"
    alexa_psl_df = eval_list4psl(alexaset, psl)
    # some subdomain analysis
    d = alexa_psl_df.psld.value_counts(
        sort=False, ascending=False, normalize=False).to_dict()
    stats["alexa_psld_stats"] = {str(k): float(v) for k, v in d.items()}
    stats["alexa_psld_len"] = len(d)
    d = alexa_psl_df.tld.value_counts(
        sort=False, ascending=False, normalize=False).to_dict()
    stats["alexa_tld_stats"] = {str(k): float(v) for k, v in d.items()}
    stats["alexa_tld_len"] = len(d)
    stats["alexa_tld_valid_len"] = len(validtlds & set(d.keys()))
    stats[L + "_tld_invalid_len"] = len(set(d.keys()) - validtlds)
    stats[L + "_tld_invalid_list"] = list(set(d.keys()) - validtlds)
    stats["alexa_tld_invalid_domaincount"] = len(alexa_psl_df)  - sum(alexa_psl_df.tld.isin(validtlds))
    d = alexa_psl_df.subdomain_depth.value_counts(
        sort=False, ascending=False, normalize=True).to_dict()
    stats["alexa_subdomain_stats"] = {int(k): float(v) for k, v in d.items()}
    try:
        stats["alexa_subdomains_but_not_basedomain"] = sorted(list(set(alexa_psl_df.basedomain.values) - set(alexa_psl_df.entry.values)))
    except TypeError as e:
        print("TypeError: {} for {} and {}".format(e, alexa_psl_df.basedomain.values, alexa_psl_df.entry.values))
    stats["alexa_subdomains_but_not_basedomain_len"] = len(stats["alexa_subdomains_but_not_basedomain"])

    if majestic:
        L = "majestic"
        majestic_psl_df = eval_list4psl(majesticset, psl)
        # majestic_psl_df.subdomain_depth.value_counts(sort=False, ascending=False,normalize=True)
        d = majestic_psl_df.psld.value_counts(
            sort=False, ascending=False, normalize=True).to_dict()
        stats["majestic_psld_stats"] = {str(k): float(v) for k, v in d.items()}
        stats["majestic_psld_len"] = len(d)
        d = majestic_psl_df.tld.value_counts(
            sort=False, ascending=False, normalize=False).to_dict()
        stats["majestic_tld_stats"] = {str(k): float(v) for k, v in d.items()}
        stats["majestic_tld_len"] = len(d)
        stats[L + "_tld_valid_len"] = len(validtlds & set(d.keys()))
        stats[L + "_tld_invalid_len"] = len(set(d.keys()) - validtlds)
        stats[L + "_tld_invalid_list"] = list(set(d.keys()) - validtlds)
        stats[L + "_tld_invalid_domaincount"] = len(majestic_psl_df)  - sum(majestic_psl_df.tld.isin(validtlds))
        d = majestic_psl_df.subdomain_depth.value_counts(
            sort=False, ascending=False, normalize=True).to_dict()
        stats["majestic_subdomain_stats"] = {int(k): float(v) for k, v in d.items()}
        stats["majestic_subdomains_but_not_basedomain"] = sorted(list(set(majestic_psl_df.basedomain) - set(majestic_psl_df.entry)))
        stats["majestic_subdomains_but_not_basedomain_len"] = len(stats["majestic_subdomains_but_not_basedomain"])

    if umbrella:
        L = "umbrella"
        umbrella_psl_df = eval_list4psl(umbrellaset, psl)
        d = umbrella_psl_df.psld.value_counts(
            sort=False, ascending=False, normalize=True).to_dict()
        stats["umbrella_psld_stats"] = {str(k): float(v) for k, v in d.items()}
        stats["umbrella_psld_len"] = len(d)
        d = umbrella_psl_df.tld.value_counts(
            sort=False, ascending=False, normalize=False).to_dict()
        stats["umbrella_tld_stats"] = {str(k): float(v) for k, v in d.items()}
        stats["umbrella_tld_len"] = len(d)
        stats["umbrella_tld_valid_len"] = len(validtlds & set(d.keys()))
        stats[L + "_tld_invalid_len"] = len(set(d.keys()) - validtlds)
        stats[L + "_tld_invalid_list"] = sorted(list(set(d.keys()) - validtlds))
        stats["umbrella_tld_invalid_domaincount"] = len(umbrella_psl_df)  - sum(umbrella_psl_df.tld.isin(validtlds))
        # umbrella_psl_df.subdomain_depth.value_counts(sort=False, ascending=False,normalize=True)
        d = umbrella_psl_df.subdomain_depth.value_counts(
            sort=False, ascending=False, normalize=True).to_dict()
        stats["umbrella_subdomain_stats"] = {int(k): float(v) for k, v in d.items()}
        umbrella_psl_df[umbrella_psl_df.subdomain_depth == 0].head(3)
        stats["umbrella_subdomains_but_not_basedomain"] = sorted(list(set(umbrella_psl_df.basedomain) - set(umbrella_psl_df.entry)))
        stats["umbrella_subdomains_but_not_basedomain_len"] = len(stats["umbrella_subdomains_but_not_basedomain"])

    # # Aggregate to base domains and intersect

    alexabaseset = set(alexa_psl_df.basedomain.values)
    stats["len_based_alexa"] = len(alexabaseset)
    if umbrella:
        umbrellabaseset = set(umbrella_psl_df.basedomain.values)
        stats["len_based_umbrella"] = len(umbrellabaseset)
        stats["corr_based_alexa_umbrella"] = len(alexabaseset & umbrellabaseset)
    if majestic:
        majesticbaseset = set(majestic_psl_df.basedomain.values)
        stats["len_based_majestic"] = len(majesticbaseset)
        stats["corr_based_alexa_majestic"] = len(alexabaseset & majesticbaseset)

    if umbrella and majestic:
        stats["corr_based_umbrella_majestic"] = len(umbrellabaseset & majesticbaseset)
        stats["corr_based_alexa_umbrella_majestic"] = len(
            umbrellabaseset & majesticbaseset & alexabaseset)

    # # SLD analysis
    # get unique SLDs -- when not normalizing to base domains, base Domains
    # with many subdomains (such as tumblr) will stand out as frequent SLDs, which is misleading
    #alexa_psl_df.to_pickle("/tmp/df.pickle")
    # d = alexa_psl_df.sld.value_counts(
    #     sort=True, ascending=False, normalize=False).to_dict()
    # alexasldset = set(alexa_psl_df.sld.values)
    # stats["alexa_sld_len"] = len(alexasldset)  # {int(k): float(v) for k, v in d.items()}
    # stats["alexa_sld"] = list(alexasldset)  # {int(k): float(v) for k, v in d.items()}
    # stats["alexa_sld_duplicates"] = {k: int(v) for k, v in d.items() if v > 1}
    # stats["alexa_subdomains_but_not_basedomain"] = set(alexa_psl_df.basedomain) - set(alexa_psl_df.entry)
    # stats["alexa_subdomains_but_not_basedomain_len"] = len(stats["alexa_subdomains_but_not_basedomain"])

    # only count every basedomain once, then get unique SLDs
    # alexa_bd_sld_df = alexa_psl_df.drop_duplicates("basedomain")["sld"]
    iter = ["alexa"]
    if umbrella:
        iter.append("umbrella")
    if majestic:
        iter.append("majestic")
    for i in iter:
        d = vars()[i + "_psl_df"].drop_duplicates("basedomain")["sld"].value_counts(
            sort=True, ascending=False, normalize=False).to_dict()
        stats[i + "_bd_sld_duplicates"] = {k: int(v) for k, v in d.items() if v > 1}
        stats[i + "_bd_sld_len"] = len(d)
        stats[i + "_bd_sld_dup_len"] = len(stats[i + "_bd_sld_duplicates"])

    with open(stats["STATSF"], 'w') as F:
        json.dump(stats, F, cls=MyEncoder, sort_keys=True)
    print("JSON dumped after {:.2f} seconds.".format(time.time() - start_time))


import datetime
from datetime import timedelta
import sys
if len(sys.argv) > 2:
    if sys.argv[2] == "1000":
        LIMIT=1000
        LIMITSTR="1k"
    else:
        LIMIT=1000000
        LIMITSTR="1M"
if len(sys.argv) > 1:
    if sys.argv[1] == "all":
        for i in range(800):
            dt = datetime.date.today() - timedelta(i)
            dt = dt.strftime("%Y-%m-%d")
            # print("Working on date (1): {}".format(dt))
            do(dt)
    else:
        do(sys.argv[1])
else:
    for i in range(14):
        dt = datetime.date.today() - timedelta(i)
        dt = dt.strftime("%Y-%m-%d")
        do(dt)
