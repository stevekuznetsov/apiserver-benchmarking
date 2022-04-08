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

from matplotlib import lines
from matplotlib import patches
from matplotlib.patheffects import withStroke

if len(sys.argv) != 3:
    print("Invalid arguments, usage:")
    print("{} [data-dir] [output-figure-dir]".format(sys.argv[0]))
    sys.exit(1)

data_dir = sys.argv[1]
output_figure_dir = sys.argv[2]
cadvisor_file_path = os.path.join(data_dir, "profiles", "cadvisor", "etcd.json")
pprof_file_path = os.path.join(data_dir, "profiles", "etcd", "heap", "pprof.json")
config_file_path = os.path.join(data_dir, "config.json")
events_file_path = os.path.join(data_dir, "events.json")

with open(cadvisor_file_path) as cadvisor_file:
    cadvisor_data = json.load(cadvisor_file)

print("loaded cadvisor")

# working_set = usage + inactive
# usage = rss + cache + swap + kernel
# rss = mapped_file + malloc
cadvisor_timestamps = []
cadvisor_data_series = {
    "rss": [],
    "cache": [],
    "swap": [],
    "mapped_file": [],
    "working_set": []
}
cadvisor_computed_data_series = {
    "kernel": [],
    "cache": [],
    "swap": [],
    "mmap": [],
    "malloc": [],
    "inactive": []
}
for item in cadvisor_data:
    cadvisor_timestamps.append(np.datetime64(item["timestamp"]))
    for field in ["rss", "cache", "swap", "mapped_file", "working_set"]:
        cadvisor_data_series[field].append(item["memory"][field])

    cadvisor_computed_data_series["cache"].append(item["memory"]["cache"])
    cadvisor_computed_data_series["swap"].append(item["memory"]["swap"])
    cadvisor_computed_data_series["mmap"].append(item["memory"]["mapped_file"])
    cadvisor_computed_data_series["malloc"].append(item["memory"]["rss"] - item["memory"]["mapped_file"])
    cadvisor_computed_data_series["kernel"].append(
        item["memory"]["usage"] - item["memory"]["rss"] - item["memory"]["cache"] - item["memory"]["swap"])
    cadvisor_computed_data_series["inactive"].append(item["memory"]["working_set"] - item["memory"]["usage"])

with open(pprof_file_path) as pprof_file:
    pprof_data = json.load(pprof_file)

print("loaded pprof")

pprof_timestamps = []
pprof_data_series = {
    "inuse_space": []  # the rest is not important now
}
for timestamp in pprof_data:
    pprof_timestamps.append(np.datetime64(timestamp))
    for field in ["inuse_space"]:
        pprof_data_series[field].append(pprof_data[timestamp][field])

with open(config_file_path) as config_file:
    config_data = json.load(config_file)

print("loaded config")

with open(events_file_path) as events_file:
    events_data = json.load(events_file)

print("loaded events")

events_timestamps = [np.datetime64(t) for t in events_data["sizeTimestamps"]]
events_data_series = events_data["sizes"]


def plot_memory_use():
    fig, ax = plt.subplots()
    ax.plot(events_timestamps, events_data_series, label="Bytes Written")
    # ax.plot(events_timestamps[0::100], events_data_series[0::100], label="Bytes Written")

    labels = ["malloc", "mmap", "cache", "swap", "kernel", "inactive"]
    ax.stackplot(cadvisor_timestamps,
                 cadvisor_computed_data_series["malloc"],
                 cadvisor_computed_data_series["mmap"],
                 cadvisor_computed_data_series["cache"],
                 cadvisor_computed_data_series["swap"],
                 cadvisor_computed_data_series["kernel"],
                 cadvisor_computed_data_series["inactive"],
                 labels=labels)

    ax.xaxis.set_major_locator(plt.MaxNLocator(3))
    # for label in cadvisor_data_series:
    #     axes.plot(cadvisor_timestamps, cadvisor_data_series[label], label=label)
    # for label in pprof_data_series:
    #     axes.plot(pprof_timestamps, pprof_data_series[label], label=label)
    ax.legend(loc="lower right")
    ax.semilogy()

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

    formatter = ticker.FuncFormatter(lambda y, pos: format(y))
    ax.yaxis.set_major_formatter(formatter)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax.set_ylim(bottom=10 * megabyte)
    ax.set_xlim(left=events_timestamps[0], right=events_timestamps[-1])
    plt.xlabel("Time")
    plt.ylabel("Memory")
    plt.title("Memory Use by CRDB")
    fig.savefig(os.path.join(output_figure_dir, "{}_memory_use.png".format(config_data["setup"]["storage_backend"])), dpi=300)
    plt.show()


# plot_memory_use()

def plot_request_times():
    print("plotting")
    del (events_data["metrics"]["list"])
    fig, ax = plt.subplots(figsize=(8, 6))
    fig.subplots_adjust(left=0.025, right=0.975)
    fig.subplots_adjust(top=0.825, bottom=0.15)
    fig.set_facecolor("w")

    ax = sns.violinplot(data=[[n / 1e6 for n in events_data["metrics"][metric]] for metric in events_data["metrics"]],
                        ax=ax, scale="width")
    print("created axes")
    ax.xaxis.set_ticks([n for n in range(len(events_data["metrics"]))])
    ax.xaxis.set_ticklabels([metric.upper() for metric in events_data["metrics"]], fontsize=16, fontweight=100)
    ax.xaxis.set_tick_params(length=6, width=1.2)
    ax.set_axisbelow(True)
    ax.grid(axis="y", color="#A8BAC4", lw=1.2)
    ax.yaxis.set_major_locator(plt.MaxNLocator(7, steps=[1, 2, 4, 5, 10]))
    ax.yaxis.set_label_position("right")
    ax.yaxis.tick_right()
    ax.yaxis.set_tick_params(labelleft=False, length=0)
    formatter = ticker.FuncFormatter(lambda y, pos: '')
    ax.yaxis.set_major_formatter(formatter)
    max_y = max([max(events_data["metrics"][metric]+[0]) for metric in events_data["metrics"]]) / 1e6
    ax.set_ylim(bottom=0, top=1.20 * max_y)
    ax.set_xlim(left=-1, right=len(events_data["metrics"]))
    PAD = max_y * 0.01
    for label in ax.get_yticks()[1:-1]:
        ax.text(
            len(events_data["metrics"]), label + PAD, str(int(label)),
            ha="right", va="baseline", fontsize=18, fontweight=100
        )

    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.spines["left"].set_visible(False)

    ax.spines["bottom"].set_lw(1.2)
    ax.spines["bottom"].set_capstyle("butt")

    text = "<size:18><weight:bold>Request Response Times,</> milliseconds</>"
    flexitext(0, 0.975, text, va="top", ax=ax)
    ax.add_artist(
        lines.Line2D(
            [0, 0.05], [1, 1], lw=2, color="black",
            solid_capstyle="butt", transform=ax.transAxes
        )
    )

    fig.text(0, 0.92, "Kubernetes API Server Performance", fontsize=22, fontweight="bold")
    fig.text(0, 0.87, "Using {} as a backing store".format(config_data["setup"]["storage_backend"]), fontsize=20)

    source = 'Client write:read ratio of 1:{}, 1GiB initial database size.\n{} requests across {} parallel workers.'.format(
        (config_data["interact"]["proportions"]["get"] + config_data["interact"]["proportions"]["list"]) / (
                    config_data["interact"]["proportions"]["create"] + config_data["interact"]["proportions"][
                "update"] + config_data["interact"]["proportions"]["delete"]),
        config_data["seed"]["count"] + config_data["interact"]["operations"],
        config_data["interact"]["parallelism"]
    )
    fig.text(0.01, 0.02, source, color="#a2a2a2", fontsize=12)
    fig.savefig(os.path.join(output_figure_dir, "{}_client_metrics.png".format(config_data["setup"]["storage_backend"])), dpi=300)
    plt.show()


plot_request_times()
