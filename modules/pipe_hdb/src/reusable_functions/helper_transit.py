


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


def pandas_add_monthdate(df, selected_col, src_format, new_colname='monthdate'):
    """pandas twin of add_monthdate - same src_format keys, so ERA_CONTRACTS works for both.

    .dt.date on the way out is REQUIRED: it lands as arrow date32 (same as spark's to_date),
    so hive folders read tx_monthdate=1990-01-01. a timestamp would write
    tx_monthdate=1990-01-01 00:00:00, miss delete_matching, and silently double every row.
    """
    import pandas as pd
    src = df[selected_col]

    # errors='raise' (pandas default) mirrors spark 4 ANSI to_date throwing on a bad value
    MONTHDATE_RECIPES = {
        'yyyy-mm':     lambda: pd.to_datetime(src, format='%Y-%m'),                                 # "2017-03"
        'yyyy-mm-dd':  lambda: pd.to_datetime(src, format='%Y-%m-%d').dt.to_period('M').dt.start_time,   # "2017-03-15"
        'dd/mm/yyyy':  lambda: pd.to_datetime(src, format='%d/%m/%Y').dt.to_period('M').dt.start_time,   # "15/03/2017"
        'mm/dd/yyyy':  lambda: pd.to_datetime(src, format='%m/%d/%Y').dt.to_period('M').dt.start_time,   # "03/15/2017"
        'timestamp':   lambda: pd.to_datetime(src).dt.to_period('M').dt.start_time,                      # "2017-03-15 14:23:00"
        # add more shapes here as new sources turn up ↓
    }

    if src_format not in MONTHDATE_RECIPES:
        raise ValueError(f"src_format {src_format!r} - known: {', '.join(MONTHDATE_RECIPES)}")
    return df.assign(**{new_colname: MONTHDATE_RECIPES[src_format]().dt.date})


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


def pandas_sortcount(df, groupcols, showtop=10):
    """pandas twin of sortcount - count per group + share of the grand total, biggest first."""
    if isinstance(groupcols, str):
        groupcols = [groupcols]

    matrix = (
        df.groupby(groupcols, dropna=False)
        .size()
        .rename('count')
        .reset_index()
        .assign(pcnt=lambda m: m['count'] / len(df))
        .sort_values('count', ascending=False, ignore_index=True)
    )
    print(matrix.head(showtop).to_string())
    return matrix
