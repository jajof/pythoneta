### Airflow DAGs

## Workspace Instructions

Before you start on your first exercise, please note the following instruction.

1. After you have updated the DAG, you will need to run `/opt/airflow/start.sh` command in the terminal of the workspace on the next page to start the Airflow webserver. See the screenshot below for the Exercise 1 Workspace.

Run /opt/airflow/start.sh command in the terminal of the workspace before you start the exercise. 

2. Wait for the Airflow web server to be ready (see screenshot below).

3. Access the Airflow UI by clicking on the blue "Access Airflow" button.

This should be able to access the Airflow UI without any delay.

## Tips for Using Airflow's Web UI

* Use Google Chrome to view the Web UI. Airflow sometimes has issues rendering correctly in Firefox or other browsers.
* Make sure you toggle the DAG to `On` before you try an run it. Otherwise you'll see your DAG running, but it won't ever finish.

__Callables__ can also be thought of as passing functions that can be included as arguments to other functions.  Examples of callables are map, reduce, filter. This is a pretty powerful feature of python you can explore more using the resources below. Callables are examples of functional programming that is introduced in an earlier lesson.

Here is the link to the [Python documentation on callables](https://docs.python.org/3.4/library/functools.html).

### Building a 

## Operators

Operators define the atomic steps of work that make up a DAG. Airflow comes with many Operators that can perform common operations. Here are a handful of common ones:
* `PythonOperator`
* `PostgresOperator`
* `RedshiftToS3Operator`
* `S3ToRedshiftOperator`
* `BashOperator`
* `SimpleHttpOperator`
* `Sensor`

## Use `@dag` Decorators to Create a DAG

A **DAG Decorator** is an annotation used to mark a function as the definition of a DAG. You can set attributes about the DAG, like: name,  description, start date, and interval. The function itself marks the beginning of the definition of the DAG.

```
import pendulum
import logging
from airflow.decorators import dag

@dag(description='Analyzes Divvy Bikeshare Data',
    start_date=pendulum.now(),
    schedule_interval='@daily')
def divvy_dag():
```

<br data-md>

## Using Operators to Define Tasks

**Operators** define the atomic steps of work that make up a DAG. Instantiated operators are referred to as **Tasks**.

```
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    print(“Hello World”)

divvy_dag = DAG(...)
task = PythonOperator(
    task_id=’hello_world’,
    python_callable=hello_world,
    dag=divvy_dag)
```

<br data-md>

Use `@task` Decorators to Define Tasks

A **Task Decorator** is an annotation used to mark a function as a **custom operator**, that generates a task.

```undefined
    @task()
    def hello_world_task():
      logging.info("Hello World")
```

<br data-md>

## Schedules

**Schedules** are optional, and may be defined with cron strings or Airflow Presets. Airflow provides the following presets:

* `@once` - Run a DAG once and then never again
* `@hourly` - Run the DAG every hour
* `@daily` - Run the DAG every day
* `@weekly` - Run the DAG every week
* `@monthly` - Run the DAG every month
* `@yearly`- Run the DAG every year
* `None` - Only run the DAG when the user initiates it

**Start Date:** If your start date is in the past, Airflow will run your DAG as many times as there are schedule intervals between that start date and the current date. 

**End Date:** Unless you specify an optional end date, Airflow will continue to run your DAGs until you disable or delete the DAG.

### Task Dependencies

### Task Dependencies 

In Airflow DAGs:
* Nodes = Tasks
* Edges = Ordering and dependencies between tasks

Task dependencies can be described programmatically in Airflow using `>>` and `<<`
* a `>>` b means a comes before b
* a `<<` b means a comes after b

```
hello_world_task = PythonOperator(task_id=’hello_world’, ...)
goodbye_world_task = PythonOperator(task_id=’goodbye_world’, ...)
...
# Use >> to denote that goodbye_world_task depends on hello_world_task
hello_world_task >> goodbye_world_task
```

Tasks dependencies can also be set with “set_downstream” and “set_upstream”
* `a.set_downstream(b)` means a comes before b
* `a.set_upstream(b)` means a comes after b

```
hello_world_task = PythonOperator(task_id=’hello_world’, ...)
goodbye_world_task = PythonOperator(task_id=’goodbye_world’, ...)
...
hello_world_task.set_downstream(goodbye_world_task)
```

### Airflow Hooks

### Connection via Airflow Hooks

Connections can be accessed in code via hooks. Hooks provide a reusable interface to external systems and databases. With hooks, you don’t have to worry about how and where to store these connection strings and secrets in your code.

```
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

def load():
# Create a PostgresHook option using the `demo` connection
    db_hook = PostgresHook(‘demo’)
    df = db_hook.get_pandas_df('SELECT * FROM rides')
    print(f'Successfully used PostgresHook to return {len(df)} records')

load_task = PythonOperator(task_id=’load’, python_callable=hello_world, ...)
```

Airflow comes with many Hooks that can integrate with common systems. Here are a few common ones: 
* `HttpHook`
* `PostgresHook` (works with RedShift)
* `MySqlHook`
* `SlackHook`
* `PrestoHook`

### Context Variables

Airflow leverages templating to allow users to “fill in the blank” with important runtime variables for tasks. We use the `**kwargs` parameter to accept the runtime variables in our task.

```
from airflow.decorators import dag, task

@dag(
  schedule_interval="@daily";
)
def template_dag(**kwargs):

  @task
  def hello_date():
    print(f“Hello {kwargs['ds']}}”)


```

(Here)[https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html] is the Apache Airflow  **Templates reference **

