from pyspark.sql.functions import struct, udf
from pyspark.sql.types import BooleanType


def get_validated_dataset(sqlContext, D, base_coin_functions, validation_to_invoke):
    def validate_witness_list(x):
        return base_coin_functions[validation_to_invoke](x['_positive_witness'], x['_negative_witness'])


    sqlContext.udf.register("validate_witness_list", validate_witness_list)
    filter_udf = udf(validate_witness_list, BooleanType())

    validated = D.filter(filter_udf(struct(D.columns)))
    keep = [x for x in validated.schema.names if x not in ['_positive_witness', '_negative_witness']]
    return validated.select(*keep)


def get_plausible_filtered(sqlContext, Dk, Dd, coin_basefunctions):
    knowledgeData = get_validated_dataset(sqlContext, Dk, coin_basefunctions, 'validatorForDk')
    knowledgeData.createOrReplaceTempView("coin_knowledge")
    plausibleData = get_validated_dataset(sqlContext, Dd, coin_basefunctions, 'validatorForDd')
    plausibleData.createOrReplaceTempView("coin_plausible")

    novelty_filter = sqlContext.sql("select a_node, b_node from coin_knowledge")
    novelty_filter.createOrReplaceTempView("coin_novelty_filter")
    plausible_filtered = sqlContext.sql(
        """
        select p.*
        from coin_plausible p
        left outer join coin_novelty_filter nf on (p.a_node = nf.a_node and p.b_node = nf.b_node)
        where nf.a_node is null and nf.b_node is null
        """)

    keep = [x for x in plausible_filtered.schema.names if x not in ['relation_identifier']]
    return plausible_filtered.select(*keep)
