'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, broadcast
from pyspark.sql.window import Window
import timeit

timer = timeit.default_timer()


spark = SparkSession.builder.appName('Optimize I').getOrCreate()


answers_input_path = 'C:\\Users\\Bob\\Desktop\\SpringBoard\\Python_Projects\\pyspark_optimization\\Optimization\\data\\answers'
questions_input_path = 'C:\\Users\\Bob\\Desktop\\SpringBoard\\Python_Projects\\pyspark_optimization\\Optimization\\data\\questions'



answersDF = spark.read.option('path', answers_input_path).load()
questionsDF = spark.read.option('path', questions_input_path).load()

# Selecting the columns that we're working with would be helpful
# ... if we working with a dataset with lots of columns. 
answersDF = answersDF.select('question_id')

'''
Answers aggregation

Here we : get number of answers per question per month
'''

answers_month = answersDF.withColumn('month', month('creation_date')).orderBy('question_id','month')

# Make this a window function. 
# ...answers_month = answersDF.groupBy('question_id', 'month').agg(count('*').alias('cnt'))

windowSpec = Window.partitionBy('question_id','month').orderBy('question_id')
answers_month = answers_month.withColumn('cnt',count('month').over(windowSpec)).select('question_id','month','cnt')

# Join coniditon filters data from right dataset. Inner join is default. 
# ...read everything from questionsDF, but only from answers_month where 
# ...question_id matches in questionsDF.  
resultDF = questionsDF.join(answers_month,['question_id']).select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF = resultDF.orderBy('question_id', 'month')
resultDF.show()



timer_end = timeit.default_timer() - timer
print('it took',timer_end,'to run the program')


'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
'''