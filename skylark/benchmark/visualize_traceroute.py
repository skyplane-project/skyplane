import graphviz  
import argparse
import json
from pprint import pprint


def main(args):

    color_map = { 
        "red": None
    }
    print("graphing")
    results = json.load(open(args.data_path))
    dot = graphviz.Digraph(comment='traceroute results')
    null_count = 0

    color_map = {
            'OrgName:        Amazon Technologies Inc.': 'powderblue', 
            'OrgName:        Google LLC': 'lightsalmon', 
            'OrgName:        Amazon.com, Inc.': 'paleturquoise', 
            'OrgName:        Amazon Data Services Ireland Limited': 'lightblue', 
            'OrgName:        Internet Assigned Numbers Authority': 'mediumpurple', 
            'OrgName:        A100 ROW GmbH': 'violet', 
            'OrgName:        Amazon Data Services Japan': 'lightcyan', 
            'OrgName:        Amazon Data Services NoVa': 'steelblue',
            'OrgName:        Amazon Data Services Brazil': 'lightskyblue',
            None: 'grey', 
        }

    colors = ["lightsalmon", "lightseagreen", "lightskyblue", "mediumpurple", "mediumseagreen", "mediumslateblue", "mediumspringgreen", "mediumturquoise", "steelblue", "tan", "teal", "thistle", "tomato"]
    color_idx = 0

    result_names = [] 

    # TODO: find some better way to choose what to graph
    for result in results[:10]: 
        src = result["src"].replace(":", "-")
        dst = result["dst"].replace(":", "-")
        result_names.append((src, dst))
        dot.node(src, style='filled',fillcolor="yellow")

        hops = result["result"]
        src_nodes = [src]
        for idx in range(len(hops)): 

            nodes = list(set([ip["ipaddr"] for ip in hops[str(idx)]]))
            next_nodes = []
            for node in hops[str(idx)]: 
                ip = node["ipaddr"]
                if ip is None: 
                    ip = f"null-{null_count}"
                    null_count += 1
                    fillcolor="grey"
                else: 
                    if node["org"] not in color_map: 
                        color_map[node["org"]] = colors[color_idx]
                        color_idx += 1
                    fillcolor = color_map[node["org"]]

                if ip in next_nodes: continue 

                dot.node(ip, style='filled',fillcolor=fillcolor) 
                for src_node in src_nodes: 
                    dot.edge(src_node, ip, label=str(idx))
                next_nodes.append(ip)
            src_nodes = next_nodes

        dot.node(dst, style='filled',fillcolor='yellow') 
        for src_node in src_nodes: 
            dot.edge(src_node, dst, label=str(len(hops)))


    print("rendering...")
    pprint(color_map)
    pprint(result_names)
    dot.render(directory='doctest-output', view=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path", type=str, default="/Users/sarahwooders/repos/skylark/data/traceroute/traceroute_2021-12-30_11-51-54.json")
    args = parser.parse_args()
    main(args)
