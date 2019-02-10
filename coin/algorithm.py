import json
import os

import numpy as np
import pyspark
from pyspark.sql.functions import col, desc

from CoinBase import *
from Engine import *
from Preprocess import *

WEIGHT = "_weight"

B_NODE = "b_node"

A_NODE = "a_node"

RESULTS_SIZE = 3
A_NODE_INDEX = 0
B_NODE_INDEX = 1
WEIGHT_INDEX = 2
LOCAL_DATA_PATH = "disease_pairs_full.txt"

DATA_SOURCE_URL = "http://snap.stanford.edu/data/facebook_combined.txt.gz"

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
with open('disease_to_numeric.json') as f:
    disease_to_numeric = json.load(f)

with open('icd_bottom_codes.json') as f:
    numeric_to_diseases = json.load(f)


def parse_string_to_numeric(string_content):
    keys = np.array(disease_to_numeric.keys())
    isin = np.flatnonzero(np.core.defchararray.find(keys, string_content) != -1)
    relevant = keys[isin]
    if not relevant:
        raise Exception("No results.")
    return disease_to_numeric[relevant[0]], relevant[0]


def run(string_content):
    try:
        content, user_input = parse_string_to_numeric(string_content)
        # urllib.urlretrieve(DATA_SOURCE_URL, LOCAL_DATA_PATH)
        sc = pyspark.SparkContext(appName="main", master='local[4]')
        sqlContext = pyspark.SQLContext(sc)
        edge_pairs = sc.textFile(LOCAL_DATA_PATH)
        Dk = getUVDFfromUndirectedEdgePairsRDD(sqlContext, edge_pairs, base_coin_functions)
        Dd = getUVSecondCircleDFfromUndirectedEdgePairsRDD(sqlContext, edge_pairs, base_coin_functions)
        res = get_plausible_filtered(sqlContext, Dk, Dd, base_coin_functions)
        res = res.filter((col(A_NODE) == content) | (col(B_NODE) == content)).sort(desc(WEIGHT)).take(4)
        results_map = get_results_map(content, user_input, res)
    except Exception as e:
        print "error -------------> \n", e
    return results_map


def get_results_map(content, user_input, res):
    result = "The top results for your search: " + user_input + ", are:\n"
    for row in res:
        try:
            if row.a_node != content:
                result = parse_row(result, row, A_NODE_INDEX)
            else:
                result = parse_row(result, row, B_NODE_INDEX)
        except Exception as e:
            print e
    return result


def parse_row(result, row, node_index):
    node_ = numeric_to_diseases[row[node_index]]
    result = add_row_to_result(node_, result, row)
    return result


def add_row_to_result(node_, result, row):
    if node_:
        result = result + "with weight = " + str(row[WEIGHT_INDEX]) + " :\n"
        for line in node_:
            result = result + line + "\n"
    return result


# print (run("Heart failure"))
# parse_string_to_numeric("Heart failure")
