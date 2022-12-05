# All-About-PageRank
Explore PageRank algorithm using different frameworks.

## Dataset
For this project, I will use a [High-energy Physics citation network](https://snap.stanford.edu/data/cit-HepPh.html) published by `Stanford University` as input graph.

As more papers are published, more edges are added to the citation graph. The dataset consists of dates when each of the paper got published as well as the citations. Using this information, we will divide the dataset into partitions depending on the year of publications, thus building an evolving graph (new edges added each year).

## Note
- 0.85 is used as damping factor for all solutions.
- PageRank score is calculated based on the formula in the original paper, which will cause the sum of all scores to be N not 1.