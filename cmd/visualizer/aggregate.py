#!/usr/bin/env python

import json
import sys
import os

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import seaborn as sns
from flexitext import flexitext
import pandas as pd

from matplotlib import lines
from matplotlib import patches
from matplotlib.patheffects import withStroke

base = "/tmp/benchmark/etcd3/cadvisor/"

data = {}
for id in [0, 1, 2]:
    with open(base+"database{}.json".format(id)) as raw:
        data[id] = json.load(raw)

fig, ax = plt.subplots(2,1)
fig.subplots_adjust(hspace=0.5)

dbs = []
cpus = []
mems = []
timestamps = []
for id in data:
    firstTime = np.datetime64(data[id][0]["timestamp"].removesuffix("Z"))
    sub_cpus = []
    sub_ts = []
    for item in data[id]:
        dbs.append("raw-"+str(id))
        mems.append(item["memory"]["working_set"])
        sub_cpus.append(item["cpu"]["usage"]["total"] / 1e9)
        sub_ts.append((np.datetime64(item["timestamp"].removesuffix("Z")) - firstTime) / np.timedelta64(1, 's'))

    grad = np.gradient(sub_cpus, sub_ts)
    cpus.extend(grad)
    timestamps.extend(sub_ts)

df = pd.DataFrame({"db": dbs, "cpu": cpus, "mem": mems, "timestamp": timestamps})

sns.lineplot(x="timestamp", y="cpu", hue="db", data=df, ax=ax[0])
sns.lineplot(x="timestamp", y="mem", hue="db", data=df, ax=ax[1])

dbs = []
cpus = []
mems = []
timestamps = []
accumMem = None
accumCpu = None
allCpu = None
for id in data:
    firstTime = np.datetime64(data[id][0]["timestamp"].removesuffix("Z"))
    sub_mems = []
    sub_cpus = []
    sub_ts = []
    for item in data[id]:
        sub_mems.append(item["memory"]["working_set"])
        cpu = float(item["cpu"]["usage"]["total"]) / 1e9
        sub_ts.append(np.datetime64(item["timestamp"].removesuffix("Z")) - firstTime)
        sub_cpus.append(cpu)

    memSeries = pd.Series(sub_mems, index=sub_ts)
    resampledMem = memSeries.resample("3S").mean().interpolate(method="time")
    if accumMem is None:
        accumMem = resampledMem
    else:
        accumMem = accumMem.add(resampledMem)

    cpuSeries = pd.Series(sub_cpus, index=sub_ts)
    resampledCpu = cpuSeries.resample("3S").mean().interpolate(method="time")
    if accumCpu is None:
        accumCpu = resampledCpu
    else:
        accumCpu = accumCpu.add(resampledCpu)

    seconds = [t / np.timedelta64(1, 's') for t in resampledCpu.index]
    grad = np.gradient(resampledCpu.values, seconds)

    for i in range(len(seconds)):
        dbs.append("resampled-"+str(id))
    cpus.extend(grad)
    timestamps.extend(seconds)
    mems.extend(resampledMem.values)

df = pd.DataFrame({"db": dbs, "cpu": cpus, "mem": mems, "timestamp": timestamps})

sns.lineplot(x="timestamp", y="cpu", hue="db", data=df, ax=ax[0], palette="pastel")
sns.lineplot(x="timestamp", y="mem", hue="db", data=df, ax=ax[1], palette="pastel")

seconds = [t / np.timedelta64(1, 's') for t in accumCpu.index]
grad = np.gradient(accumCpu.values, seconds)

mem = pd.DataFrame({"mem": accumMem.values, "cpu": grad, "timestamp": [t / np.timedelta64(1, 's') for t in accumMem.index]})
sns.lineplot(x="timestamp", y="mem", data=mem, ax=ax[1], label="foo")

plt.show()

fig, ax = plt.subplots(2,1)
fig.subplots_adjust(hspace=0.5)

seconds = [t / np.timedelta64(1, 's') for t in accumCpu.index]
rawcpudf = pd.DataFrame({"cpu": accumCpu.values, "timestamp": seconds})
sns.lineplot(x="timestamp", y="cpu", data=rawcpudf, ax=ax[0], label="summed")

sns.lineplot(x="timestamp", y="cpu", data=mem, ax=ax[1], label="summed")

plt.show()
