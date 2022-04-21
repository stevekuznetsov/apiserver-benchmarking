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

config_data = {}
events_data = {}
for data_dir in sys.argv[1:3]:
    config_file_path = os.path.join(data_dir, "config.json")
    events_file_path = os.path.join(data_dir, "events.json")

    with open(config_file_path) as config_file:
        data = json.load(config_file)
        identifier = "native"
        if "selectivity" in data["interact"]:
            identifier = "indexed"
        config_data[identifier] = data

    with open(events_file_path) as events_file:
        events_data[identifier] = json.load(events_file)


def parse_request_times(data):
    dbs = []
    durations = []
    for id in data:
        cutoff = np.quantile(data[id]["metrics"]["create"], 0.985)
        for value in data[id]["metrics"]["create"]:
            if value >= cutoff:
                continue
            dbs.append(id)
            durations.append(value / 1e6)

    return pd.DataFrame({"db": dbs, "duration": durations})


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
    df["dummy"] = ""
    sns.violinplot(x="dummy", y="duration", hue='db', data=df, split=True,
                   ax=ax, scale="width", inner=None, hue_order=df.db.unique())
    annotate_axis(
        ax=ax,
        xticks=[],
        xticklabels=[],
        max_y=df.duration.max(),
        max_x=1,
        ytickformat=lambda y: "{:,.0f}".format(y),
        title="<size:18><weight:bold>Insertion Latency,</> milliseconds</>"
    )

    path_effects = [withStroke(linewidth=10, foreground="white")]
    idx = 0
    first = True
    dbIdx = 0
    for db in df.db.unique():
        values = df.loc[(df['db'] == db)]
        if first:
            layout_args = {
                "x": idx - .4,
                "y": values.median(),
                "ha": "right",
            }
        else:
            layout_args = {
                "x": idx + .4,
                "y": values.median(),
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


one = 1
thousand = 1000 * one
million = 1000 * thousand
billion = 1000 * million


def formatCount(count):
    if count < thousand:
        return str(count)
    elif thousand <= count < million:
        return '{:.0f}'.format(count / thousand) + 'k'
    elif million <= count < billion:
        return '{:.0f}'.format(count / million) + 'M'
    else:
        return '{:.0f}'.format(count / billion) + 'B'


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


fig, axes = plt.subplots(figsize=(8, 6))
plot_request_times(axes, parse_request_times(events_data))
fig.subplots_adjust(
    left=0, right=1,
    top=0.825, bottom=0.15,
    hspace=0.25, wspace=0.05
)
fig.set_facecolor("w")
title = "<size:22><weight:bold>Insertion Latency For Pods\n</></>" + \
        "<size:20>Investigating indexing in crdb</>"
flexitext(0, .98, title, va="top", xycoords='figure fraction', ax=fig.axes[0])

source = '{} initial database size comprised of {} objects.\n' \
         'Insertions executed across {} parallel workers.'.format(
    formatBytes(
        config_data["native"]["seed"]["count"] *
        config_data["native"]["seed"]["fill_size"]),
    formatCount(config_data["native"]["seed"]["count"]),
    config_data["native"]["seed"]["parallelism"]
)
fig.text(0.01, 0.02, source, color="#a2a2a2", fontsize=12)
fig.savefig(
    os.path.join(output_figure_dir, "insertion_latencies.png"),
    dpi=300)
plt.show()
plt.show()
