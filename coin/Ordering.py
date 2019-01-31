def sort_by_jaccard(sqlContext, df, source_col, dest_col, entity_to_search=None):
    """
         Creates RDD of the UV class the user provides.

         @param sqlContext                 - The current active SQL context.
         @param df                         - Dataframe the user wants to convert to relations of class UV
         @param source_col                 - The DF column to treat as source node.
         @param dest_col                   - The DF column to treat as destination node.
         @return RDD of the UV class the user provides
    """

    def jaccardSimilarity(firstList, secondList):
        firstSet = set(firstList)
        secondSet = set(secondList)
        union = len(firstSet.union(secondSet))
        intersection = len(firstSet.intersection(secondSet))

        return float(intersection) / union

    sqlContext.udf.register("jaccard_calc", jaccardSimilarity)

    df.createOrReplaceTempView("plausible_filtered")
    params = dict(source_col=source_col, dest_col=dest_col, entity_to_search=entity_to_search)
    sqlContext.sql(
        """
        with extended_title as (
        select {source_col} as title,
               {dest_col} as ext
      from plausible_filtered x
      where {source_col} = '{entity_to_search}'
      union all
      select {dest_col} as title,
             {source_col} as ext
      from plausible_filtered x
      where {dest_col} = '{entity_to_search}'
     )
      select distinct title, ext
      from extended_title
      """.format(**params)).createOrReplaceTempView("title_extended")

    sqlContext.sql(
        """
          with title_exp as (
              select title, ext, {dest_col} as exp from plausible_filtered p join title_extended e on (p.{source_col} = e.title)
              union all
              select title, ext, {source_col} as exp from plausible_filtered p join title_extended e on (p.{dest_col} = e.title)
              ),
              ext_exp as (
              select title,ext, {dest_col} as exp from plausible_filtered p join title_extended e on (p.{source_col} = e.ext)
              union all
              select title,ext, {source_col} as exp from plausible_filtered p join title_extended e on (p.{dest_col} = e.ext)
              ),
              title_agg as (select title, ext, collect_set(exp) as col from title_exp group by title, ext),
              ext_agg as (select title, ext, collect_set(exp) as col from ext_exp group by title, ext)
              select te.title, te.ext, jaccard_calc(te.col, texp.col) as sim
              from title_agg te
              join ext_agg texp on (te.title = texp.title and te.ext = texp.ext)
              """.format(**params)).createOrReplaceTempView("relations_with_jaccard")
    retdf = sqlContext.sql(
        """
            select pf.*, j.sim as jaccard_sim
            from plausible_filtered pf
            join relations_with_jaccard j on (least(pf.{source_col}, pf.{dest_col}) = least(j.title, j.ext)
                                              and greatest(pf.{source_col}, pf.{dest_col}) = greatest(j.title, j.ext))
          order by j.sim desc
        """.format(**params))
    return retdf
