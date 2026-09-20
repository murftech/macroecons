


# from pympler import asizeof
# asizeof.asizeof(era_dfs) 

# def memsize(python_object):

#     total_bytes = sum(t.nbytes for t in arrow_tables)
#     mb = total_bytes / 1e6 


def add_monthdate(df, selected_col, src_format, new_colname='monthdate'):

    from sparkutils.functions import col, to_date, F
    src = col(selected_col)

    MONTHDATE_RECIPES = {
        'yyyy-mm':     lambda: to_date(src, 'yyyy-MM'),                             # "2017-03"
        'yyyy-mm-dd':  lambda: F.trunc(to_date(src, 'yyyy-MM-dd'), 'month'),       # "2017-03-15"
        'dd/mm/yyyy':  lambda: F.trunc(to_date(src, 'dd/MM/yyyy'), 'month'),       # "15/03/2017"
        'mm/dd/yyyy':  lambda: F.trunc(to_date(src, 'MM/dd/yyyy'), 'month'),       # "03/15/2017"
        'timestamp':   lambda: F.date_trunc('month', src.cast('timestamp')).cast('date'),  # "2017-03-15 14:23:00"
        # add more shapes here as new sources turn up ↓
    }

    if src_format not in MONTHDATE_RECIPES:
        raise ValueError(f"src_format {src_format!r} - known: {', '.join(MONTHDATE_RECIPES)}")
    return df.withColumn(new_colname, MONTHDATE_RECIPES[src_format]())


def sortcount(df, groupcols, showtop=10):
    """count per group + each group's share of the grand total, biggest first.

    polars' `count / count.sum()` has no direct Spark equivalent: an aggregate
    over the WHOLE result is a window with no partition key, and Spark warns
    loudly about that because it funnels every row through one partition. So take
    the grand total with a plain count() and divide by a literal.
    """
    from sparkutils.functions import col, lit

    if isinstance(groupcols, str):
        groupcols = [groupcols]

    total = df.count()
    matrix = (
        df.groupBy(*groupcols)
        .count()
        .withColumn('pcnt', col('count') / lit(total))
        .orderBy(col('count').desc())
    )
    matrix.show(showtop, truncate=False)
    return matrix
