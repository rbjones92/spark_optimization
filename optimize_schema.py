# Robert Jones
# 4.6.22
# Springboard mini-project. Optimize pyspark code 

'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import timeit
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType, TimestampType
from pyspark.sql.functions import col, count, month, broadcast


load_timer = timeit.default_timer()


spark = SparkSession.builder.appName('Optimize I').getOrCreate()

answers_input_path = 'C:\\Users\\Bob\\Desktop\\SpringBoard\\Python_Projects\\pyspark_optimization\\Optimization\\spark_optimize\\data\\answers'
questions_input_path = 'C:\\Users\\Bob\\Desktop\\SpringBoard\\Python_Projects\\pyspark_optimization\\Optimization\\spark_optimize\\data\\questions'

answers_schema = StructType([
 StructField('question_id',LongType(),False),
 StructField('answer_id',IntegerType(),False),
 StructField('creation_date',TimestampType(),False),
 StructField('comments',StringType(),False),
 StructField('user_id',IntegerType(),False),
 StructField('score',IntegerType(),False),
 ])


questions_schema = StructType([
StructField('question_id',LongType(),False),
StructField('element',StringType(),False),
StructField('creation_date',TimestampType(),False),
StructField('title',StringType(),False),
StructField('accepted_answer_id',LongType(),True),
StructField('comments',IntegerType(),False),
StructField('user_id',IntegerType(),False),
StructField('views',IntegerType(),False),
])

# Problems: Infers schema, doesn't designate format, 
# ...answersDF = spark.read.option('path', answers_input_path).load()
# Solution: Define schema and filetype
answersDF = spark.read.format('parquet').options(header=True).load(answers_input_path, schema=answers_schema)


# Problems: Infers schema, doesn't designate format, 
# ...questionsDF = spark.read.option('path', questions_input_path).load()

# Solution: Define schema and filtype
questionsDF = spark.read.format('parquet').options(header=True).load(questions_input_path, schema=questions_schema)

'''
Answers aggregation

Here we : get number of answers per question per month
'''

''' 
Time Saving Techniques:
1. Define schema
2. Define filetype
'''

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))


resultDF = questionsDF.join(answers_month,['question_id'])

resultDF = resultDF.select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF = resultDF.orderBy('question_id', 'month')

resultDF.show()

load_timer_end = timeit.default_timer() - load_timer

print('it took',load_timer_end,'to run the program')

