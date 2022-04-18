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

if len(sys.argv) != 4:
    print("Invalid arguments, usage:")
    print("{} [data-dir-a] [data-dir-b] [output-figure-dir]".format(sys.argv[0]))
    sys.exit(1)

output_figure_dir = sys.argv[3]

cadvisor_data = {}
config_data = {}
events_data = {}
for data_dir in sys.argv[1:3]:
    config_file_path = os.path.join(data_dir, "config.json")
    events_file_path = os.path.join(data_dir, "events.json")
    cadvisor_file_path = os.path.join(data_dir, "profiles", "cadvisor", "etcd.json")

    with open(config_file_path) as config_file:
        data = json.load(config_file)
        identifier = data["setup"]["storage_backend"]
        config_data[identifier] = data

    with open(events_file_path) as events_file:
        events_data[identifier] = json.load(events_file)

    with open(cadvisor_file_path) as cadvisor_file:
        cadvisor_data[identifier] = json.load(cadvisor_file)


def annotate_axis(ax, xticks, xticklabels, max_y, max_x, ytickformat, title):
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.xaxis.set_ticks(xticks)
    ax.xaxis.set_ticklabels(xticklabels, fontsize=16, fontweight=100)
    ax.xaxis.set_tick_params(length=6, width=1.2)
    ax.set_axisbelow(True)
    ax.grid(axis="y", color="#A8BAC4", lw=1.2)
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_locator(plt.MaxNLocator(6, steps=[1, 2, 4, 5, 10]))
    ax.yaxis.set_label_position("right")
    ax.yaxis.tick_right()
    ax.yaxis.set_tick_params(labelleft=False, length=0)
    formatter = ticker.FuncFormatter(lambda y, pos: '')
    ax.yaxis.set_major_formatter(formatter)
    ax.set_xlim(left=-1, right=max_x)
    PAD = max_y * 0.01
    for label in ax.get_yticks()[1:-1]:
        ax.text(
            max_x, label + PAD, ytickformat(label),
            ha="right", va="baseline", fontsize=18, fontweight=100
        )
    plt.legend(loc="lower right", fontsize=12, frameon=False)

    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.get_legend().remove()

    ax.spines["bottom"].set_lw(1.2)
    ax.spines["bottom"].set_capstyle("butt")

    flexitext(0, 1, title, va="bottom", ax=ax)
    ax.add_artist(
        lines.Line2D(
            [0, 0.05], [1, 1], lw=2, color="black",
            solid_capstyle="butt", transform=ax.transAxes
        )
    )


def plot_request_times(ax):
    dbs = []
    methods = []
    durations = []
    for id in events_data:
        for metric in events_data[id]["metrics"]:
            if len(events_data[id]["metrics"][metric]) == 0:
                continue
            cutoff = np.quantile(events_data[id]["metrics"][metric], 0.95)
            for value in events_data[id]["metrics"][metric]:
                if value >= cutoff:
                    continue
                dbs.append(id)
                methods.append(metric)
                durations.append(value / 1e6)

    df = pd.DataFrame({"db": dbs, "method": methods, "duration": durations})

    ax = sns.violinplot(x="method", y="duration", hue='db', data=df, split=True,
                        ax=ax, scale="width", inner=None, hue_order=df.db.unique())
    annotate_axis(
        ax=ax,
        xticks=[n for n in range(len(df.method.unique()))],
        xticklabels=[metric.upper() for metric in df.method.unique()],
        max_y=df.duration.max(),
        max_x=len(df.method.unique()),
        ytickformat=lambda y: str(int(y)),
        title="<size:18><weight:bold>Request Response Times,</> milliseconds</>"
    )

    path_effects = [withStroke(linewidth=10, foreground="white")]
    idx = len(df.method.unique()) - 1
    method = df.method.unique()[idx]
    first = True
    dbIdx = 0
    for db in df.db.unique():
        values = df.loc[(df['db'] == db) & (df['method'] == method)]
        midpoint = values.duration.median()
        if first:
            layout_args = {
                "x": idx + 0.1,
                "ha": "left",
            }
        else:
            layout_args = {
                "x": idx - 0.1,
                "ha": "right",
            }
        ax.text(
            y=midpoint, s=db, fontsize=18, color=sns.color_palette()[dbIdx],
            va="center", path_effects=path_effects, **layout_args
        )
        dbIdx+=1
        first = False


kilobyte = 1000
megabyte = 1000 * kilobyte
gigabyte = 1000 * megabyte


def format(bytes):
    if 0 < bytes < megabyte:
        return '{:,.0f}'.format(bytes / kilobyte) + 'KB'
    elif megabyte <= bytes < gigabyte:
        return '{:,.0f}'.format(bytes / megabyte) + 'MB'
    else:
        return '{:,.0f}'.format(bytes / gigabyte) + 'GB'


def plot_resource_usage(ax):
    dbs = []
    cpus = []
    mems = []
    timestamps = []
    for id in cadvisor_data:
        base = np.datetime64(cadvisor_data[id][0]["timestamp"].removesuffix("Z"))
        sub_cpus = []
        sub_ts = []
        for item in cadvisor_data[id]:
            dbs.append(id)
            mems.append(item["memory"]["working_set"])
            sub_cpus.append(item["cpu"]["usage"]["total"] / 1e9)
            sub_ts.append((np.datetime64(item["timestamp"].removesuffix("Z")) - base) / np.timedelta64(1, 's'))

        grad = np.gradient(sub_cpus, sub_ts)
        cpus.extend(grad)
        timestamps.extend(sub_ts)

    df = pd.DataFrame({"db": dbs, "cpu": cpus, "mem": mems, "timestamp": timestamps})

    sns.lineplot(x="timestamp", y="cpu", hue="db", data=df, ax=ax[0])
    annotate_axis(
        ax=ax[0],
        xticks=[],
        xticklabels=[],
        max_y=df.cpu.max(),
        max_x=df.timestamp.max() * 1.05,
        ytickformat=lambda y: "{:0.1f}".format(y),
        title="<size:18><weight:bold>CPU Usage,</> vCPUs</>"
    )

    sns.lineplot(x="timestamp", y="mem", hue="db", data=df, ax=ax[1])
    annotate_axis(
        ax=ax[1],
        xticks=[],
        xticklabels=[],
        max_y=df.mem.max(),
        max_x=df.timestamp.max() * 1.05,
        ytickformat=lambda y: format(y),
        title="<size:18><weight:bold>Working Set,</> bytes</>"
    )

    path_effects = [withStroke(linewidth=10, foreground="white")]

    idx = 0
    for db in df.db.unique():
        values = df.loc[df['db'] == db]
        midpoint = values[values['timestamp'] == values['timestamp'].quantile(q=0.45, interpolation='nearest')]
        va = {
            "crdb": "top",
            "etcd3": "bottom",
        }
        multiplier = {
            "crdb": 0.9,
            "etcd3": 1.1,
        }
        ax[0].text(
            midpoint.timestamp, multiplier[db] * midpoint.cpu, db, fontsize=18,
            va=va[db], ha="left", path_effects=path_effects, color=sns.color_palette()[idx]
        )
        ax[1].text(
            midpoint.timestamp, multiplier[db] * midpoint.mem, db, fontsize=18,
            va=va[db], ha="left", path_effects=path_effects, color=sns.color_palette()[idx]
        )
        idx+=1


fig, axes = plt.subplot_mosaic([['left', 'upper right'],
                                ['left', 'lower right']],
                               figsize=(12, 7.2), constrained_layout=True)
plot_request_times(axes["left"])
plot_resource_usage([axes["upper right"], axes["lower right"]])
fig.subplots_adjust(
    left=0, right=1,
    top=0.825, bottom=0.15,
    hspace=0.25, wspace=0.05
)
fig.set_facecolor("w")
title = "<size:22><weight:bold>Kubernetes API Server Performance\n</></>" + \
        "<size:20>Comparing etcd3 and crdb as backing stores</>"
flexitext(0, .98, title, va="top", xycoords='figure fraction', ax=fig.axes[0])

proportions = config_data["crdb"]["interact"]["throughput"]
source = 'Client write:read ratio of 1:{:,.0f}, {} initial database size.\n{} requests across {} parallel workers.'.format(
    (proportions["get"]) / (proportions["create"] + proportions["update"] + proportions["delete"]),
    format(config_data["crdb"]["seed"]["count"] * config_data["crdb"]["seed"]["fill_size"]),
    config_data["crdb"]["seed"]["count"] + config_data["crdb"]["interact"]["operations"],
    config_data["crdb"]["interact"]["parallelism"]
)
fig.text(0.01, 0.02, source, color="#a2a2a2", fontsize=12)
fig.savefig(
    os.path.join(output_figure_dir, "comparative_plot.png"),
    dpi=300)
plt.show()
