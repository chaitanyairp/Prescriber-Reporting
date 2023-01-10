from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


@udf(returnType=IntegerType())
def split_count_udf(colm):
    return len(colm.split(" "))
