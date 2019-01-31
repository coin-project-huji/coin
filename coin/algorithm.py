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
    a_nodes = [np.array([int(row.a_node) for row in res])]
    b_nodes = [np.array([int(row.b_node) for row in res])]
    weights = [np.array([int(row._weight) for row in res])]
    return np.concatenate((a_nodes, b_nodes, weights), axis=0).transpose()


print (run("1024"))