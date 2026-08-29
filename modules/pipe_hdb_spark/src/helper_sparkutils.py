#############
# import All
#############
import pyspark.sql.functions as F
import pyspark.sql.types as T


# note that whatever below has to pass [test for colliding functions]
#############
# import base
#############
from pyspark.sql.functions import col, lit, when, to_date, year, bround, count, median
# these are free bro.
#############
# import extras
#############
from pyspark.sql.window import Window







#### below are learning wheels, can be deleted ####

# ### test for colliding functions (Learning point)
# import builtins
# import pyspark.sql.functions as F

# collisions = sorted(set(dir(builtins)).intersection(set(dir(F))))
# print("Colliding Functions:", [f for f in collisions if not f.startswith("_")])
# Colliding Functions: ['abs', 'ascii', 'bin', 'filter', 'hash', 'hex', 'max', 'min', 'pow', 'round', 'slice', 'sum']
# all these common functions must use via F:
# F.abs, F.max, F.min, F.round, F.sum


# def load_namespace():

#     #############
#     # import All
#     #############
#     import pyspark.sql.functions as F
#     import pyspark.sql.types as T


#     # note that whatever below has to pass [test for colliding functions]
#     #############
#     # import base
#     #############
#     from pyspark.sql.functions import col, lit, when, to_date, year

#     #############
#     # import extras
#     #############
#     from pyspark.sql.window import Window
#     from pyspark.sql.functions import bround, count, median

#     sys._getframe(1).f_globals.update(locals())

