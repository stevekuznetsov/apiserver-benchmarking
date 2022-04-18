#!/usr/bin/env python

import json
import os
import sys

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import seaborn as sns
from flexitext import flexitext
from matplotlib import lines
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

    with open(config_file_path) as config_file:
        data = json.load(config_file)
        identifier = data["setup"]["storage_backend"]
        config_data[identifier] = data

    with open(events_file_path) as events_file:
        events_data[identifier] = json.load(events_file)


def parse_request_times(data):
    dbs = []
    listSizes = []
    durations = []
    for id in data:
        for listSize in data[id]["indexedMetrics"]["list"]:
            if len(data[id]["indexedMetrics"]["list"][listSize]) == 0:
                continue
            for value in data[id]["indexedMetrics"]["list"][listSize]:
                dbs.append(id)
                listSizes.append(listSize)
                durations.append(np.log10(value))

    return pd.DataFrame({"db": dbs, "listSize": listSizes, "duration": durations})


def annotate_axis(ax, xticks, xticklabels, max_y, max_x, ytickformat, title):
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.xaxis.set_ticks(xticks)
    ax.xaxis.set_ticklabels(xticklabels, fontsize=16, fontweight=100)
    ax.xaxis.set_tick_params(length=6, width=1.2)
    ax.set_axisbelow(True)
    ax.grid(axis="y", color="#A8BAC4", lw=1.2)
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


def plot_request_times(ax, df):
    sns.violinplot(x="listSize", y="duration", hue='db', data=df, split=True,
                   ax=ax, scale="width", inner=None, hue_order=df.db.unique())
    annotate_axis(
        ax=ax,
        xticks=[n for n in range(len(df.listSize.unique()))],
        xticklabels=["{:f}".format(100*float(metric)/float(config_data["crdb"]["interact"]["selectivity"]["count"])).rstrip('0').rstrip('.') + "%" for metric in df.listSize.unique()],
        max_y=df.duration.max(),
        max_x=len(df.listSize.unique()),
        ytickformat=lambda y: format(y),
        title="<size:18><weight:bold>Request Response Times,</> seconds</>"
    )

    path_effects = [withStroke(linewidth=10, foreground="white")]
    idx = len(df.listSize.unique()) - 3
    listSize = df.listSize.unique()[idx]
    first = True
    dbIdx = 0
    for db in df.db.unique():
        values = df.loc[(df['db'] == db) & (df['listSize'] == listSize)]
        if first:
            layout_args = {
                "x": idx,
                "y": 1.025 * values.duration.max(),
                "ha": "right",
            }
        else:
            layout_args = {
                "x": idx,
                "y": .975 * values.duration.min(),
                "ha": "left",
            }
        ax.text(
            s=db, fontsize=18, color=sns.color_palette()[dbIdx],
            va="center", path_effects=path_effects, **layout_args
        )
        dbIdx += 1
        first = False


nanosecond = 1
microsecond = 1000 * nanosecond
millisecond = 1000 * microsecond
second = 1000 * millisecond


def format(nanoseconds):
    nanoseconds = np.power(10, nanoseconds)
    if second <= nanoseconds:
        return '{:.0f}'.format(nanoseconds / second) + 's'
    elif millisecond <= nanoseconds < second:
        return '{:.0f}'.format(nanoseconds / millisecond) + 'ms'
    elif microsecond <= nanoseconds < millisecond:
        return '{:.0f}'.format(nanoseconds / microsecond) + 'us'
    else:
        return '{:.0f}'.format(nanoseconds / nanosecond) + 'ns'


kilobyte = 1000
megabyte = 1000 * kilobyte
gigabyte = 1000 * megabyte


def formatBytes(bytes):
    if 0 < bytes < megabyte:
        return '{:,.0f}'.format(bytes / kilobyte) + 'KB'
    elif megabyte <= bytes < gigabyte:
        return '{:,.0f}'.format(bytes / megabyte) + 'MB'
    else:
        return '{:,.0f}'.format(bytes / gigabyte) + 'GB'


df = parse_request_times(events_data)
print(df.duration.min())
fig, ax = plt.subplots(figsize=(8, 6))
fig.subplots_adjust(
    left=0, right=1,
    top=0.825, bottom=0.15,
    hspace=0.25, wspace=0.05
)
plot_request_times(ax, df)
fig.set_facecolor("w")
title = "<size:22><weight:bold>Kubernetes Selective LIST Performance\n</></>" + \
        "<size:20>Comparing etcd3 and crdb as backing stores</>"
flexitext(0, .98, title, va="top", xycoords='figure fraction', ax=fig.axes[0])

source = '{} initial database size.\n{} requests across {} parallel workers.'.format(
    formatBytes(
        config_data["crdb"]["interact"]["selectivity"]["count"] *
        config_data["crdb"]["interact"]["selectivity"]["fill_size"]),
    config_data["crdb"]["interact"]["operations"],
    config_data["crdb"]["interact"]["parallelism"]
)
fig.text(0.01, 0.02, source, color="#a2a2a2", fontsize=12)
fig.savefig(
    os.path.join(output_figure_dir, "indexed_requests.png"),
    dpi=300)
plt.show()
plt.show()
