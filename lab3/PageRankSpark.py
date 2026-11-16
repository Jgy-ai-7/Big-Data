# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext

INPUT = "hdfs://localhost:9000/pagerank/links.txt"

conf = SparkConf().setAppName("PageRank").setMaster("local[*]")
sc = SparkContext(conf=conf)

lines = sc.textFile(INPUT)
links = (lines.map(lambda l: l.strip().split("\t"))
         .filter(lambda x: len(x) >= 2 and x[1].strip() != "")
         .map(lambda x: (x[0], [t.strip() for t in x[1].split(",") if t.strip() != ""])))

links = links.cache()
n = links.count()
pr = links.mapValues(lambda v: 1.0 / n)

for i in range(10):
    contrib = links.join(pr).flatMap(
        lambda x: [(t, x[1][1] / float(len(x[1][0]))) for t in x[1][0]]
    )
    newpr = contrib.reduceByKey(lambda a, b: a + b).mapValues(lambda v: 0.15 / n + 0.85 * v)
    pr = newpr.rightOuterJoin(links).mapValues(lambda v: v[0] if v[0] is not None else 0.0)
    print("iteration %d done" % (i + 1))

result = pr.sortBy(lambda x: -x[1]).collect()
for page, p in result:
    print("(%s,%.10f)" % (page, p))

sc.stop()
