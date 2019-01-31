def extract_special_functions(special_fields):
    res = []
    for name in special_fields:
        funcname = name.split("_")[1]

        currcol = sqlfunctions_dict(name, funcname)
        if len(currcol) > 0:
            res.append(currcol)

    if len(res) > 0:
        return ", " + ", ".join(res)
    else:
        return ""


def sqlfunctions_dict(name, funcname):
    funcsdict = {
        "weight": "count(*) as " + name,
        "sum": "sum(" + name.replace("_sum_", "") + ") as " + name,
        "avg": "avg(" + name.replace("_avg_", "") + ") as " + name,
        "count": "count(" + name.replace("_count_", "") + ") as " + name,
        "min": "min(" + name.replace("_min_", "") + ") as " + name,
        "max": "max(" + name.replace("_max_", "") + ") as " + name
    }
    if funcname in funcsdict:
        return funcsdict[funcname]
    else:
        return ""


def getUVDFfromDF(sqlContext, df, source_col, dest_col, funcs, garbage_relation_threshold=10000,
                  special_fields=None):
    """
         Creates RDD of the UV class the user provides.

         @param sqlContext                 - The current active SQL context.
         @param df                         - Dataframe the user wants to convert to relations of class UV
         @param source_col                 - The DF column to treat as source node.
         @param dest_col                   - The DF column to treat as destination node.
         @param funcs                      - COIN core functions to extract insights.
         @param special_fields             - Special fields to include in SQL result as special function results
         @param garbage_relation_threshold - Ignore relation over this weight (To decrease memory pressure)
         @tparam UV - The case class that extends CoinType to convert the dataframe into.
         @return RDD of the UV class the user provides
    """

    if special_fields is None:
        special_fields = ['_weight']
    neededlist = [x for x in df.schema.names if x not in [source_col, dest_col]]

    def positive_witness(x):
        return funcs['positiveWitnessForDk'](x)

    def negative_witness(x):
        return funcs['negativeWitnessForDk'](x)

    sqlContext.udf.register("positive_witness", positive_witness)
    sqlContext.udf.register("negative_witness", negative_witness)

    except_special = [x for x in neededlist if not x.startswith("_")]
    maxfields = get_max_fields(except_special)
    df.createOrReplaceTempView("temptable")
    params = {"source_col": source_col, "dest_col": dest_col,
              "without_special": get_all_without_special(except_special),
              "dummy_aggregation": maxfields,
              "aggregated_fields": extract_special_functions(special_fields),
              "garbage_relation_threshold": garbage_relation_threshold}
    sql_text = """
       with cast_nodes as (
           select 
           cast({source_col} as string) _first_node, 
           cast({dest_col} as string) _second_node
           {without_special}
           from temptable t
       ),
       with_ids as (
           select
           least(_first_node, _second_node) as a_node,
           greatest(_first_node, _second_node) as b_node
           {without_special}
           from cast_nodes
       ),
       grouped_by_ids_witnesses as (
           select --_rid,
                  a_node, b_node,
                  collect_list(positive_witness(struct(*))) as _positive_witness,
                  collect_list(negative_witness(struct(*))) as _negative_witness
                  {dummy_aggregation} {aggregated_fields}
           from with_ids group by a_node, b_node having count(*) < {garbage_relation_threshold}
       )
       select * from grouped_by_ids_witnesses
    """.format(**params)
    res = sqlContext.sql(sql_text)
    return res


def get_max_fields(except_special):
    if len(except_special) > 0:
        return "," + ", ".join([" first(" + x + ") as " + x for x in except_special])
    else:
        return ""


def get_all_without_special(except_special):
    if len(except_special) > 0:
        return "," + ",".join(except_special)
    else:
        return ""


def get_nodes_from_edge(e):
    return e.srcId, e.dstId


def getUVSecondCircleDFfromUndirectedEdgePairsRDD(sqlContext, edgepairs, funcs):
    def parsefile(x):
        vals = x.split(" ")
        return str(vals[0]), str(vals[1])

    edgepairs.map(parsefile).toDF().createOrReplaceTempView("edges")
    dataDf = sqlContext.sql("""
          with edges as (select _1 as first_node, _2 as second_node from edges),
          -- join edges to itself so that the dest of left table is source of right table, to create a len 2 trajectory. eliminate trajectories that are actually len 1 in orig table and get distinct results.
           distinct_second_circle as (select  e1.first_node, e2.second_node
          from edges as e1
          join edges as e2 on (e1.second_node = e2.first_node)
          ),
          res as (
            select cast(first_node as string) first_node, 
                   cast(second_node as string) second_node from distinct_second_circle
          )
          select concat(least(first_node, second_node), '_', greatest(first_node, second_node)) as _rid, *
          from res
          -- originally there are 2690019 edges, about 250k of which are 2nd degree only. This will be our Du.
          """)
    return getUVDFfromDF(sqlContext, dataDf, "first_node", "second_node", funcs)


def getUVDFfromUndirectedEdgePairsRDD(sqlContext, edgepairs, funcs, directed=True):
    def parsefile(x):
        vals = x.split(" ")
        return str(vals[0]), str(vals[1])

    edgepairs.map(parsefile).toDF().createOrReplaceTempView("edges")
    dataDf = sqlContext.sql("select _1 as first_node, _2 as second_node from edges")

    return getUVDFfromDF(sqlContext, dataDf, "first_node", "second_node", funcs)
