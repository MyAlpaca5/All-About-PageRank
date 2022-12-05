from collections import defaultdict
from datetime import datetime
import pathlib
import shutil
from typing import Dict, Tuple, List, Set


FILE_DATES = 'cit-HepPh-dates.txt'
FILE_CITATION = 'cit-HepPh.txt'
DIR_BATCH = 'batch'
DIR_INCREMENTAL = 'incremental'


def parseFiles() -> Tuple[Dict[int, List[str]], Dict[str, Set[str]]]:
    # parse cit-HepPh-dates.txt

    # Note: leading 11 indicates cross list, needed to be removed, and original node it
    # should be 7 digits, I will padding zeros if necessary.
    # For example, '119812022' -> '9812022', '1110055' -> '0010055'

    # Note: special cross list for year 2000, January to September, used 011 prefix
    # For example, '0115083	2000-05-19'

    node_date = dict()
    year_nodes = defaultdict(list)
    with open(FILE_DATES, mode="r") as f:
        for line in f:
            line = line.strip()
            if line.startswith("#") or line == "":
                continue

            node, date = line.split()
            date = datetime.strptime(date, "%Y-%m-%d")

            if node.startswith('11'):
                node = node[2:]
            elif node.startswith('011') and date in []:
                node = node[3:]

            node = node.zfill(7)
            node_date[node] = date
            year_nodes[date.year].append(node)

    # parse cit-HepPh.txt

    # Note: the nodes should be 7 digits, but the nodes in the file removed leading zeros,
    # therefore, for any nodes that has length less than 7, I will padding zeros.

    citations = defaultdict(list)
    with open(FILE_CITATION, mode="r") as f:
        for line in f:
            line = line.strip()
            if line.startswith("#") or line == "":
                continue

            src, dst = line.split()
            src, dst = src.zfill(7), dst.zfill(7)

            if dst not in node_date or src not in node_date:
                # if a paper cites a paper that is not in the HepPh group, skip it
                # or if a paper doesn't have a date entry, skip it
                continue

            if node_date[src] < node_date[dst]:
                # there are some wierd data in the file, for example '9502363 103230'
                # a 1995 year paper cites a 2001 paper, doesn't make sense, so I will
                # not include those data.
                continue

            citations[src].append(dst)

    return year_nodes, citations


def batch(year_nodes: Dict[int, List[str]], citations: Dict[str, Set[str]]):
    path = pathlib.Path.cwd() / DIR_BATCH
    if path.exists():
        shutil.rmtree(path.absolute().as_posix())
    path.mkdir(parents=True, exist_ok=True)

    cumulative_citations = []
    for year in range(1992, 2003):
        for node in year_nodes[year]:
            for citing in citations[node]:
                cumulative_citations.append((node, citing))

        citation_path = path / f"{year}-edges.txt"
        with open(citation_path, mode="w") as f:
            for node, citing in cumulative_citations:
                f.write(f"{node} {citing}\n")


def incremental(year_nodes: Dict[int, List[str]], citations: Dict[str, Set[str]]):
    path = pathlib.Path.cwd() / DIR_INCREMENTAL
    if path.exists():
        shutil.rmtree(path.absolute().as_posix())
    path.mkdir(parents=True, exist_ok=True)

    for year in range(1992, 2003):
        incremental_citations = []
        for node in year_nodes[year]:
            for citing in citations[node]:
                incremental_citations.append((node, citing))

        citation_path = path / f"{year}-edges.txt"
        with open(citation_path, mode="w") as f:
            for node, citing in incremental_citations:
                f.write(f"{node} {citing}\n")


if __name__ == '__main__':
    year_nodes, citations = parseFiles()
    batch(year_nodes, citations)
    incremental(year_nodes, citations)
