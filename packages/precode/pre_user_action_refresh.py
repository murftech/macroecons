# from pyspark.sql.functions import col
# from pyspark.sql.types import *
# from pyspark.sql.window import Window


##############
# pre wrappers for user refresh
#############

def coltype_comparison (df_a, df_b, prikey):
    # Collect dtypes into dict for quick lookup
    types_a = dict(df_a.dtypes)
    types_b = dict(df_b.dtypes)

    # Build comparison rows
    rows = []
    for target_col in prikey:
        type_a = types_a.get(target_col)
        type_b = types_b.get(target_col)
        rows.append(Row(
            col_name=target_col,
            df_a_type=type_a,
            df_b_type=type_b,
            match_flag=(type_a == type_b)
        ))

    # Create a summary DataFrame
    summary_df = spark.createDataFrame(rows)
    summary_df.show(truncate=False)

    return summary_df


def timestamp_duplicate(filepath):

    from pathlib import Path
    from datetime import datetime
    import shutil
    import re

    #0. get the file extension
    ext= re.search('\..+$', filepath).group(0)

    # 1. Create timestamp suffix

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 2. Build new path with timestamp before extension
    new_path = filepath + '_' + timestamp + ext

    # 3. Copy file
    shutil.copy2(filepath, new_path)

    print("Duplicated file in the same folder to:", new_path)



def refresh_rows_user_actions(newdata, mnl_data, prikey, user_given_fields, refresh_now, datecol, orderby=None):
    ### function ###
    fixed_col_order = mnl_data.columns
    mnl_data = mnl_data.orderBy([col(c).desc() for c in prikey])
    mnl_data.show()

    # remove computed fields from refreshdata
    mnl_data_user_only = mnl_data.select(prikey + user_given_fields)
    mnl_data_user_only.show()

    newdata.printSchema()

    print('railguard 1: the join keys needs to match typing')

    typer = coltype_comparison(newdata, mnl_data, prikey)
    typer.show()

    if (typer.filter(~col('match_flag')).count() > 0):
        raise Exception('given typing is not matching')

    if (typer.filter(~col('match_flag')).count() == 0):
        print('join keys type match passed')

    print('railguard 2: here if the joins are nothing, something is terribly wrong.')
    dc.in_a_b(newdata, mnl_data_user_only, prikey)

    newdata_plus_user_fields = newdata.join(mnl_data_user_only, on=prikey, how='left')

    # register the current column order
    print('NOTE: if i require new computed columns, please define them in the csv first')
    refreshed_data = newdata_plus_user_fields.select(fixed_col_order)
    # refreshed_data = refreshed_data.distinct()
    # what is this distinct for
    # i should not distinct it. And if i dont distinct it will i keep having more and more rows?
    

    if orderby==None:
        refreshed_data = refreshed_data.orderBy([col(c).desc() for c in prikey])
    else:
        print('reach this stage?')
        refreshed_data = refreshed_data.orderBy([col(c).desc() for c in orderby])

    refreshed_data.agg(max(datecol)).show()
    refreshed_data.show()
    dc.dim(refreshed_data)

    if refresh_now == 'y':
        print('replace = yes given, hence update spark_write_csv done at the same time`')
        timestamp_duplicate(refreshpath)
        rw.spark_write_csv(refreshed_data, refreshpath)

    return refreshed_data

############


