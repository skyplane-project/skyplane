import graphviz  
import argparse
import json


def main(args):
    results = json.load(open(args.data_path))
    dot = graphviz.Digraph(comment='traceroute results')

    for result in results[:10]: 
        src = result["src"].replace(":", "-")
        dst = result["dst"].replace(":", "-")
        dot.node(src, shape='star')
        #dot.node(dst)

        hops = result["result"]
        src = [src]
        for idx in range(len(hops)): 
            if idx == 0: continue 

            new_src = []
            for ip in hops[str(idx)]: 
                if ip["ipaddr"] is None: continue
                node = ip["ipaddr"]
                dot.node(node)
                new_src.append(node)
                for s in src: 
                    dot.edge(s, node)
            src = list(set(new_src))

        dot.node(dst, shape='star')
        for s in src: 
            dot.edge(s, dst)


    print("rendering...")
    dot.render(directory='doctest-output', view=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path", type=str)
    args = parser.parse_args()
    main(args)
