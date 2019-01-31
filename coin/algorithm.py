import urllib
from Engine import *
from CoinBase import *
from Preprocess import *
import numpy as np
from pyspark.sql.functions import desc, col
import pyspark
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.8.0-openjdk-amd64"


def run(content):
    urllib.urlretrieve("http://snap.stanford.edu/data/facebook_combined.txt.gz", "facebook_combined.txt.gz")
    sc = pyspark.SparkContext(appName="main", master='local[4]')
    sqlContext = pyspark.SQLContext(sc)
    edge_pairs = sc.textFile("facebook_combined.txt.gz")
    Dk = getUVDFfromUndirectedEdgePairsRDD(sqlContext, edge_pairs, base_coin_functions)
    Dd = getUVSecondCircleDFfromUndirectedEdgePairsRDD(sqlContext, edge_pairs, base_coin_functions)
    res = get_plausible_filtered(sqlContext, Dk, Dd, base_coin_functions)
    res = res.filter((col("a_node") == content) | (col("b_node") == content)).sort(desc("_weight")).take(3)
    # rows = res.select('a_node').collect()
    a_nodes = np.array([int(row.a_node) for row in res])
    # res = res.select('b_node').collect()
    b_nodes = np.array([int(row.b_node) for row in res])
    # res = res.select('_weight').collect()
    weights = np.array([int(row._weight) for row in res])
    return np.hstack((a_nodes, b_nodes, weights)).transpose().ravel()


print (run("1024"))