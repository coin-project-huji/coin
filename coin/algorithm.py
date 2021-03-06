import csv
import json
import os

import numpy as np
import pyspark
from pyspark.sql.functions import desc, col

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
    disease_to_numeric = dict((k.lower(), v) for k, v in disease_to_numeric.iteritems())

with open('icd_bottom_codes.json') as f:
    numeric_to_full_diseases = json.load(f)

with open('icd_top_codes.json') as f:
    numeric_to_disease = json.load(f)
    numeric_to_disease = dict((k, v.lower()) for k, v in numeric_to_disease.iteritems())


def parse_string_to_numeric(string_content):
    keys = np.array(disease_to_numeric.keys())
    isin = np.flatnonzero(np.core.defchararray.find(keys, string_content) != -1)
    relevant = keys[isin]
    if relevant.size == 0:
        raise Exception("We are sorry, but no results found. Please try to be more specific.")
    return disease_to_numeric[relevant[0]], relevant[0]



def writeDBResource(res):
    res = res.sort(desc(WEIGHT)).take(5000)
    max_weight = res[0][WEIGHT_INDEX]
    to_add_row = []
    with open('people.csv', 'w') as writeFile:
        writer = csv.writer(writeFile)
        for row in res:
            to_add_row.append(str(numeric_to_disease[row[A_NODE_INDEX]]))
            to_add_row.append(str(numeric_to_disease[row[B_NODE_INDEX]]))
            to_add_row.append(float(row[WEIGHT_INDEX]) / float(max_weight))
            writer.writerows([to_add_row])
            to_add_row = []


def run(string_content):
    try:
        content, user_input = parse_string_to_numeric(string_content)
        sc = pyspark.SparkContext.getOrCreate()
        sqlContext = pyspark.SQLContext(sc)
        edge_pairs = sc.textFile(LOCAL_DATA_PATH)
        Dk = getUVDFfromUndirectedEdgePairsRDD(sqlContext, edge_pairs, base_coin_functions)
        Dd = getUVSecondCircleDFfromUndirectedEdgePairsRDD(sqlContext, edge_pairs, base_coin_functions)
        res = get_plausible_filtered(sqlContext, Dk, Dd, base_coin_functions)
        writeDBResource(res)
        print ("-----------------------------saved into people--------------------------------")

        res = res.filter((col(A_NODE) == content) | (col(B_NODE) == content)).sort(desc(WEIGHT)).take(4)
        results_map = get_results_map(content, user_input, res)
        return results_map
    except Exception as e:
        print ("error -------------> \n", e)
        return e.__str__()


def get_results_map(content, user_input, res):
    if not res:
        return "We are sorry, but no results found"
    result = "The top results for your search: " + user_input + ", are:\n"
    for row in res:
        try:
            if row.a_node != content:
                result = parse_row(result, row, A_NODE_INDEX)
            else:
                result = parse_row(result, row, B_NODE_INDEX)
        except Exception as e:
            print (e)
    return result


def parse_row(result, row, node_index):
    node_ = numeric_to_full_diseases[row[node_index]]
    result = add_row_to_result(node_, result, row)
    return result


def add_row_to_result(node_, result, row):
    if node_:
        result = result + "with weight = " + str(row[WEIGHT_INDEX]) + " :\n"
        for line in node_:
            result = result + line + "\n"
    return result
