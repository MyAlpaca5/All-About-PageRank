import heapq
import networkx as nx
from pathlib import Path


data_path = Path('../dataset/batch')

if __name__ == "__main__":

    for year in range(1992, 2003):
        edges_file = str(year) + '-edges.txt'
        edges_path = data_path / edges_file

        graph = nx.DiGraph()

        # populate the graph
        with open(edges_path) as e:
            for line in e:
                src, dst = line.split()
                src, dst = int(src), int(dst)
                graph.add_edge(src, dst)

        # print out dangling nodes
        # print([node for node in graph.nodes if graph.out_degree(node) == 0])

        # calculate pagerank
        ranks = nx.pagerank(graph, alpha=0.85, tol=1e-10)

        # normalize
        numOfNodes = graph.number_of_nodes()
        correctionFactor = numOfNodes / 1.0
        ranks = {node: rank * correctionFactor for node, rank in ranks.items()}

        topKRanks = heapq.nlargest(5, ranks.items(), key=lambda x: x[1])
        print(f'--- year {year} top 5 ---')
        for vert, rank in topKRanks:
            print(f'{vert} has rank: {rank}')
