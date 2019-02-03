import urllib
from Engine import *
from CoinBase import *
from Preprocess import *
import numpy as np
from pyspark.sql.functions import desc, col
import pyspark
import os

WEIGHT = "_weight"

B_NODE = "b_node"

A_NODE = "a_node"

RESULTS_SIZE = 3

LOCAL_DATA_PATH = "facebook_combined.txt.gz"

DATA_SOURCE_URL = "http://snap.stanford.edu/data/facebook_combined.txt.gz"

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"


def run(content):
    try:
        urllib.urlretrieve(DATA_SOURCE_URL, LOCAL_DATA_PATH)
        sc = pyspark.SparkContext(appName="main", master='local[4]')
        sqlContext = pyspark.SQLContext(sc)
        edge_pairs = sc.textFile(LOCAL_DATA_PATH)
        Dk = getUVDFfromUndirectedEdgePairsRDD(sqlContext, edge_pairs, base_coin_functions)
        Dd = getUVSecondCircleDFfromUndirectedEdgePairsRDD(sqlContext, edge_pairs, base_coin_functions)
        res = get_plausible_filtered(sqlContext, Dk, Dd, base_coin_functions)
        res = res.filter((col(A_NODE) == content) | (col(B_NODE) == content)).sort(desc(WEIGHT)).take(3)
        titles = [np.array([A_NODE, B_NODE, WEIGHT])]
        a_nodes = [np.array([int(row.a_node) for row in res])]
        b_nodes = [np.array([int(row.b_node) for row in res])]
        weights = [np.array([int(row._weight) for row in res])]
        # return np.concatenate((titles, np.concatenate((a_nodes, b_nodes, weights), axis=0).transpose()), axis=0)[0]
        res = np.concatenate((titles, np.concatenate((a_nodes, b_nodes, weights), axis=0).transpose()), axis=0)
    except Exception as e:
        print "error -------------> \n", e
    return prettify_table(res)


def prettify_table(table_arrays):
    return '\n'.join(' '.join(map(str, sl)) for sl in table_arrays).replace("[", "").replace("]", "")


# print (run("1746"))
