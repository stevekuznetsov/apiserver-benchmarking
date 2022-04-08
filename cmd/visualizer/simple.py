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

if len(sys.argv) != 2:
    print("Invalid arguments, usage:")
    print("{} [data-file]".format(sys.argv[0]))
    sys.exit(1)

times = {}
data = {}
with open(sys.argv[1]) as data_file:
    for line in data_file.readlines():
        raw = json.loads(line.lstrip("BENCHMARK: "))
        method = raw["method"]
        if method not in times:
            times[method] = []
        if method not in data:
            data[method] = []
        times[method].append(np.datetime64(raw["timestamp"]))
        data[method].append(raw["duration"] / 1e6)

fig, ax = plt.subplots()
ax = sns.violinplot(data=[data[method] for method in data], ax=ax, scale="width", palette="pastel")
ax.xaxis.set_ticks([n for n in range(len(data))])
labels = {
    "CREATE": "INSERT",
    "DELETE": "DELETE",
    "UPDATE": "UPDATE",
    "GET": "SELECT",
}
ax.xaxis.set_ticklabels([labels[metric] for metric in data])
plt.ylabel("Time (ms)")
plt.title("Request Durations")
plt.show()

# fig, ax = plt.subplots()
# for method in times:
#     ax.scatter(times[method], data[method], label=method)
# ax.legend(loc="upper left")
# ax.xaxis.set_major_locator(plt.MaxNLocator(3))
# ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
# plt.ylabel("Requests Duration (ms)")
# plt.xlabel("Request Index")
# plt.title("Request Durations")
# plt.show()

# def plot_request_times():
#     print("plotting")
#     del (events_data["metrics"]["list"])
#     fig, ax = plt.subplots(figsize=(8, 6))
#     fig.subplots_adjust(left=0.025, right=0.975)
#     fig.subplots_adjust(top=0.825, bottom=0.15)
#     fig.set_facecolor("w")
#
#     ax = sns.violinplot(data=[[n / 1e6 for n in events_data["metrics"][metric]] for metric in events_data["metrics"]],
#                         ax=ax, scale="width")
#     print("created axes")
#     ax.xaxis.set_ticks([n for n in range(len(events_data["metrics"]))])
#     ax.xaxis.set_ticklabels([metric.upper() for metric in events_data["metrics"]], fontsize=16, fontweight=100)
#     ax.xaxis.set_tick_params(length=6, width=1.2)
#     ax.set_axisbelow(True)
#     ax.grid(axis="y", color="#A8BAC4", lw=1.2)
#     ax.yaxis.set_major_locator(plt.MaxNLocator(7, steps=[1, 2, 4, 5, 10]))
#     ax.yaxis.set_label_position("right")
#     ax.yaxis.tick_right()
#     ax.yaxis.set_tick_params(labelleft=False, length=0)
#     formatter = ticker.FuncFormatter(lambda y, pos: '')
#     ax.yaxis.set_major_formatter(formatter)
#     max_y = max([max(events_data["metrics"][metric]) for metric in events_data["metrics"]]) / 1e6
#     ax.set_ylim(bottom=0, top=1.20 * max_y)
#     ax.set_xlim(left=-1, right=len(events_data["metrics"]))
#     PAD = max_y * 0.01
#     for label in ax.get_yticks()[1:-1]:
#         ax.text(
#             len(events_data["metrics"]), label + PAD, str(int(label)),
#             ha="right", va="baseline", fontsize=18, fontweight=100
#         )
#
#     ax.spines["right"].set_visible(False)
#     ax.spines["top"].set_visible(False)
#     ax.spines["left"].set_visible(False)
#
#     ax.spines["bottom"].set_lw(1.2)
#     ax.spines["bottom"].set_capstyle("butt")
#
#     text = "<size:18><weight:bold>Request Response Times,</> milliseconds</>"
#     flexitext(0, 0.975, text, va="top", ax=ax)
#     ax.add_artist(
#         lines.Line2D(
#             [0, 0.05], [1, 1], lw=2, color="black",
#             solid_capstyle="butt", transform=ax.transAxes
#         )
#     )
#
#     fig.text(0, 0.92, "Kubernetes API Server Performance", fontsize=22, fontweight="bold")
#     fig.text(0, 0.87, "Using {} as a backing store".format(config_data["setup"]["storage_backend"]), fontsize=20)
#
#     source = 'Client write:read ratio of 1:{}, 1GiB initial database size.\n{} requests across {} parallel workers.'.format(
#         (config_data["interact"]["proportions"]["get"] + config_data["interact"]["proportions"]["list"]) / (
#                     config_data["interact"]["proportions"]["create"] + config_data["interact"]["proportions"][
#                 "update"] + config_data["interact"]["proportions"]["delete"]),
#         config_data["seed"]["count"] + config_data["interact"]["operations"],
#         config_data["interact"]["parallelism"]
#     )
#     fig.text(0.01, 0.02, source, color="#a2a2a2", fontsize=12)
#     fig.savefig(os.path.join(output_figure_dir, "{}_client_metrics.png".format(config_data["setup"]["storage_backend"])), dpi=300)
#     plt.show()
#
#
# plot_request_times()
