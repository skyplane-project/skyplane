from pprint import pprint
import shutil
import networkx as nx
import graphviz as gv
import json
import pandas as pd
from random import randint
from skyplane.utils import logger
import functools


@functools.lru_cache(maxsize=None)
def get_path_cost(src, dst, src_tier="PREMIUM", dst_tier="PREMIUM"):
    from skyplane.compute.cloud_provider import CloudProvider

    assert src_tier == "PREMIUM" and dst_tier == "PREMIUM"
    return CloudProvider.get_transfer_cost(src, dst)


def make_nx_graph(cost, throughput):
    G = nx.DiGraph()
    for _, row in throughput.iterrows():
        if row["src_region"] == row["dst_region"]:
            continue
        G.add_edge(row["src_region"], row["dst_region"], cost=None, throughput=row["throughput_sent"] / 1e9)

    for _, row in cost.iterrows():
        if row["src"] in G and row["dest"] in G[row["src"]]:
            G[row["src"]][row["dest"]]["cost"] = row["cost"]
        else:
            continue

    # update the cost using skyplane.compute tools [i.e. in utils.py] (need to build the skyplane repo first)
    for edge in G.edges.data():
        if edge[-1]["cost"] is None:
            edge[-1]["cost"] = get_path_cost(edge[0], edge[1])

    assert all([edge[-1]["cost"] is not None for edge in G.edges.data()])
    return G


def plot_children(h, start_node, region, li, partition_id):
    # list of dicts
    for i in li:
        children = i["children"]

        if i["op_type"] == "send":
            assert len(children) == 0
            if "region" not in i:
                print("What is i?", i)
            children = [{"op_type": "receive", "region": i["region"], "children": []}]

        if i["op_type"] == "receive" and "region" in i:
            this_node = i["region"] + "/" + i["op_type"]
        else:
            this_node = region + "/" + i["op_type"]
            # if i["op_type"] == "send":
            # this_node += i["handle"].split("_")[-1]

        h = plot_children(h, this_node, region, children, partition_id)

        if h.has_edge(start_node, this_node):
            h[start_node][this_node]["partition"].append(partition_id)
        else:
            h.add_edge(start_node, this_node, partition=[partition_id])
    return h


def get_nx_graph(path):
    # if dot is not installed
    has_dot = shutil.which("dot") is not None
    if not has_dot:
        logger.error("Graphviz is not installed. Please install it to plot the solution (sudo apt install graphviz).")
        return None

    nx_g = nx.DiGraph()
    start_node = "start"
    nx_g.add_node(start_node)
    with open(path, "r") as f:
        data = json.load(f)
        regions = list(data.keys())

        # for region in regions
        for region in regions:
            # region = regions[0]
            region_p = data[region]
            print("Region: ", region)
            pprint(region_p)

            for partition_group in region_p:
                partitions = partition_group["partitions"]
                p_plan = partition_group["value"]
                for pid in partitions:
                    nx_g = plot_children(nx_g, start_node, region, p_plan, pid)

            # pprint(region_p)
            # plans = region_p["_plan"]
            # partitions = [int(i) for i in list(plans.keys())]
            ## print("partitions: ", [int(i) for i in list(plans.keys())])
            # for pid in partitions:
            #    p_plan = plans[str(pid)]
            #    nx_g = plot_children(nx_g, start_node, region, p_plan, pid)
            ## break

        # print(partitions)

    add_nx_g = nx.DiGraph()
    for edge in nx_g.edges:
        s, d = edge[0], edge[1]
        partition = list(set(nx_g[s][d]["partition"]))
        add_nx_g.add_edge(s, d, partition=partition)
        s_region = s.split("/")[-1][0]
        if s.split("/")[-1].startswith("mux") and d.split("/")[-1] == "receive":
            # print("Receive!:", d)
            send_node = s_region + "/send"
            add_nx_g.add_edge(s, send_node)
            add_nx_g.add_edge(send_node, d)
    add_nx_g.remove_node(start_node)
    print(add_nx_g.edges.data())

    # nx.draw(add_nx_g, with_labels=True)
    return add_nx_g


from pprint import pprint
import shutil
import networkx as nx
import graphviz as gv
import json


def networkx_to_graphviz(g, label="partition"):
    """Convert `networkx` graph `g` to `graphviz.Digraph`.

    @type g: `networkx.Graph` or `networkx.DiGraph`
    @rtype: `graphviz.Digraph`
    """
    if g.is_directed():
        h = gv.Digraph()
    else:
        h = gv.Graph()
    # h.attr(rankdir="LR")

    for u, d in g.nodes(data=True):
        # print(u, d)
        a = u
        # u = u.split(",")[0]
        u = u.replace("/", ":")
        u = u.replace(":", " ")
        f = color_map[a.split("/")[0]]
        if u.endswith("mux_and") or u.endswith("mux_or"):
            f = "gray"
        h.node(str(u), fillcolor=f, style="filled")
    for u, v, d in g.edges(data=True):
        # print('edge', u, v, d)
        u_r = u.replace("/", ":")
        u_r = u_r.replace(":", " ")
        v_r = v.replace("/", ":")
        v_r = v_r.replace(":", " ")
        h.edge(str(u_r), str(v_r), label=str(d[label]))
    return h


if __name__ == "__main__":
    costs = pd.read_csv("broadcast/profiles/cost.csv")
    throughput = pd.read_csv("broadcast/profiles/throughput.csv")

    color = []
    n = 100
    for i in range(n):
        color.append("#%06X" % randint(0, 0xFFFFFF))

    complete = make_nx_graph(costs, throughput)
    color_map = {}
    nodes = list(complete.nodes)
    for i in range(len(nodes)):
        color_map[nodes[i]] = color[i]

    # plot_path = "/tmp/skyplane/gw_programs/gateway_programs_complete.json"
    plot_path = "/Users/sarahwooders/repos/skyplane/new_gw_programs/gateway_programs_complete.json"
    add_nx_g = get_nx_graph(plot_path)
    h = networkx_to_graphviz(add_nx_g)
    h.view()
