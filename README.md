## spark_optimization

## Summary
This project is an optimization exercise of existing pyspark code. 

## Description
This project involves reviewing pyspark code that extracts and transforms using pyspark SQL. Methods attempted consisted of defining schema, using window functions, and brodcast joins.

## Technologies
- Python 3.7
- Pyspark 2.4.4
- VS Code

## Local Execution
Before Optimization
![Alt Text](screenshot/optimize_initial.JPG?raw=true "load output")

Optimizing by defining schema
![Alt Text](screenshot/optimize_schema.JPG?raw=true "load output")

Optimizing by using window function
![Alt Text](screenshot/after_optimize.JPG?raw=true "load output")

## Findings
I found that I wasn't able to optimize this code much. This could be due to the initial code being speedy, and the dataset not being large enough to reap the benefits of 
standard spark speedups. 
