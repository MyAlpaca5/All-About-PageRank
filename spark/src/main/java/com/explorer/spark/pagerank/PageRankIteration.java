package com.explorer.spark.pagerank;

// based on the naive solution provided by Spark.
// https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/JavaPageRank.java

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public class PageRankIteration {

    private static final Pattern SPACES = Pattern.compile("\\s+");
    private static final String DATA_DIR_PATH = "../dataset/batch";

    private static class MyTupleComparator implements
            Comparator<Tuple2<String, Double>>, Serializable {
        final static MyTupleComparator INSTANCE = new MyTupleComparator();

        @Override
        public int compare(Tuple2<String, Double> t1, Tuple2<String, Double> t2) {
            return t1._2.compareTo(t2._2); // sort descending
            // return -t1._2.compareTo(t2._2); // sort ascending
        }
    }

    private static class Add implements Function2<Double, Double, Double> {
        final static Add INSTANCE = new Add();

        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    private static JavaPairRDD<String, Double> NormalizeRanks(JavaPairRDD<String, Double> ranks) {
        Double sum = ranks.values().reduce(Add.INSTANCE);
        Double normalized_factor = ranks.count() / sum;
        return ranks.mapValues(v -> v * normalized_factor);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PageRankIteration <end_year> <iteration>");
            System.exit(1);
        }

        int end_year = Integer.parseInt(args[0]);
        int iteration = Integer.parseInt(args[1]);

        SparkSession spark = SparkSession
                .builder()
                .appName("PageRankIteration")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        for (int year = 1992; year <= end_year; year++) {
            Path edge_path = Paths.get(DATA_DIR_PATH, Integer.toString(year) + "-edges.txt");

            // loads in input file. It should be in format of:
            // vert neighbor_vert
            // ...
            JavaRDD<String> edge_lines = spark.read().textFile(edge_path.toString()).javaRDD();

            JavaPairRDD<String, Iterable<String>> edges = edge_lines.mapToPair(line -> {
                String[] verts = SPACES.split(line);
                return new Tuple2<>(verts[0], verts[1]);
            }).groupByKey().cache();

            JavaPairRDD<String, Double> ranks = edge_lines.flatMapToPair(s -> {
                List<Tuple2<String, Double>> results = new ArrayList<>();
                String[] verts = SPACES.split(s);
                results.add(new Tuple2<>(verts[0], 1.0));
                results.add(new Tuple2<>(verts[1], 1.0));
                return results.iterator();
            }).distinct();

            JavaPairRDD<String, Double> base_contribs = ranks.mapValues(v -> 0.0).cache();

            // calculates and updates ranks continuously using PageRank algorithm
            for (int i = 0; i < iteration; i++) {
                // calculates contributions to the rank of other vertices
                JavaPairRDD<String, Double> new_contribs = edges.join(ranks).values()
                        .flatMapToPair(verts_rank -> {
                            List<Tuple2<String, Double>> results = new ArrayList<>();
                            int dest_vert_count = Iterables.size(verts_rank._1());
                            for (String dest_vert : verts_rank._1) {
                                results.add(new Tuple2<>(dest_vert, verts_rank._2() / dest_vert_count));
                            }
                            return results.iterator();
                        });

                // recalculate the new ranks
                ranks = base_contribs.union(new_contribs).reduceByKey(Add.INSTANCE)
                        .mapValues(sum -> 0.15 + sum * 0.85);
            }

            ranks = NormalizeRanks(ranks);
            List<Tuple2<String, Double>> top_ranks = ranks.top(5, MyTupleComparator.INSTANCE);
            System.out.printf("--- year %d top 5 ---\n", year);
            top_ranks.forEach(rank -> System.out.printf("%s has rank: %.12f \n", rank._1(), rank._2()));
        }

        spark.stop();
    }
}
