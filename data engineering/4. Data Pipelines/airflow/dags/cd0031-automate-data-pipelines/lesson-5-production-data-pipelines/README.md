# Airflow Plugins

Airflow was built with the intention of allowing its users to extend and customize its functionality through plugins. The most common types of user-created plugins for Airflow are Operators and Hooks. These plugins make DAGs reusable and simpler to maintain.

To create custom operator, follow the steps:

1. Identify Operators that perform similar functions and can be consolidated
1. Define a new Operator in the plugins folder
1. Replace the original Operators with your new custom one, re-parameterize, and instantiate them.

<a href="https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html" target="_blank">Here</a> is the Official Airflow Documentation for custom operators

### Operator Plugins

## Exercise Instructions
In this exercise, we’ll consolidate repeated code into Operator Plugins
- Move the greater_than_zero check logic into a custom operator
- Replace both uses of the PythonOperator with the new custom operator
- Execute the DAG


# Task Boundaries
DAG tasks should be designed such that they are:
* Atomic and have a single purpose
* Maximize parallelism
* Make failure states obvious

Every task in your dag should perform **only one job.**

> “Write programs that do one thing and do it well.” - Ken Thompson’s Unix Philosophy

### Benefits of Task Boundaries
* Re-visitable: Task boundaries are useful for you if you revisit a pipeline you wrote after a 6 month absence. You'll have a much easier time understanding how it works and the lineage of the data if the boundaries between tasks are clear and well defined. This is true in the code itself, and within the Airflow UI.
* Tasks that do just one thing are often more easily parallelized. This parallelization can offer a significant speedup in the execution of our DAGs.

## Exercise Instructions
In this exercise, we’ll refactor a DAG with a single overloaded task into a DAG with several tasks with well-defined boundaries
- Read through the DAG and identify points in the DAG that could be split apart
- Split the DAG into multiple PythonOperators
- Run the DAG

# SubDAGs
Commonly repeated series of tasks within DAGs can be captured as reusable SubDAGs. Benefits include:
* Decrease the amount of code we need to write and maintain to create a new DAG
* Easier to understand the high level goals of a DAG
* Bug fixes, speedups, and other enhancements can be made more quickly and distributed to all DAGs that use that SubDAG

## Drawbacks of Using SubDAGs

* Limit the visibility within the Airflow UI
* Abstraction makes understanding what the DAG is doing more difficult
* Encourages premature optimization

### Common Questions
**Can Airflow nest subDAGs?** -  Yes, you can nest subDAGs. However, you should have a really good reason to do so because it makes it much harder to understand what's going on in the code. Generally, subDAGs are not necessary at all, let alone subDAGs within subDAGs.

## Exercise Instructions
In this exercise, we’ll place our S3 to RedShift Copy operations into a SubDag.
- Consolidate HasRowsOperator into the SubDag
- Reorder the tasks to take advantage of the SubDag Operators

# Building a Full DAG

In this exercise you will construct a DAG and custom operator end-to-end on your own. Our bikeshare company would like to create a trips facts table every time we update the trips data. You've decided to make the facts table creation a custom operator so that it can be reused for other tables in the future.

The skeleton of the custom operator, as well as the facts SQL statement has been created for you and can be found in `plugins/operators/facts_calculator.py`. The DAG itself will be defined in `dags/lesson4-production-data-pipelines/starter/l4_e4_build_full_dag.py`.

Using the previous exercises as examples, follow the instructions in the DAG and Operator file to complete the exercise. 

## Exercise Instructions

Create a DAG which performs the following functions:

- Loads Trip data from S3 to RedShift
- Performs a data quality check on the Trips table in RedShift
- Uses the FactsCalculatorOperator to create a Facts table in Redshift. NOTE: to complete this step you must complete the FactsCalcuatorOperator skeleton defined in plugins/operators/facts_calculator.py
