### Add Airflow Connections

Here, we'll use Airflow's UI to configure your AWS credentials.

1. To go to the Airflow UI:
   * In a classroom workspace, run the script `/opt/airflow/start.sh`. Once you see the message "Airflow web server is ready" click on the blue **Access Airflow** button in the bottom right. 
2. Click on the **Admin** tab and select **Connections**.
3. Under **Connections**, click the plus button.
4. On the create connection page, enter the following values:
   * **Connection Id**: Enter `aws_credentials`.
   * **Connection Type**: Enter `Amazon Web Services`.
   * **Login**: Enter your **Access key ID** from the IAM User credentials you downloaded earlier.
   * **Password**: Enter your **Secret access key** from the IAM User credentials you downloaded earlier.
   Once you've entered these values, select **Save**.

> **Note**: The **Access key ID** and **Secret access key** should be taken from the **csv** file you downloaded after creating an IAM User on the page **Create an IAM User in AWS**.  
>


> The **Access key ID** and **Secret access key** shown when you click on the **Launch Cloud Gateway** button in the classroom will NOT work.  
>

This should connect Airflow to AWS. We will use this connection in the next few demos and exercises.

### Copy S3 Data

The CSV data for the next few exercises is stored in Udacity's S3 bucket. This bucket is in the US West AWS Region. To simplify things, we are going to copy the data to your own bucket, so Redshift can access the bucket.

<br data-md>

Using the AWS Cloudshell, create your own S3 bucket (buckets need to be unique across all AWS accounts): `aws s3 mb s3://sean-murdock/`

<br data-md>

Copy the data from Udacity's bucket to your own bucket: `aws s3 cp s3://udacity-dend/divvy/ s3://sean-murdock/divvy/ --recursive`

<br data-md>

Update  `/udacity/common/sql_statements.py` to use the new bucket. Copy `/udacity/common/sql_statements.py` to the dag directory.

List the data to be sure it copied over: `aws s3 ls s3://sean-murdock/divvy/`

### Connections and Hooks

* Open the Airflow UI and open Admin->Variables
* Click "Create"
* Set `Key` equal to `s3_bucket` and set `Value` equal to **your bucket name**
* Set `Key` equal to `s3_prefix` and set `Value` equal to `data-pipelines`
* Click save
* Run the DAG

### Build the S3 to Redshift DAG

# MetastoreBackend

The `MetastoreBackend` python class connects to the **Airflow Metastore Backend** to retrieve credentials and other data needed to connect to outside systems. 

The below code creates an `aws_connection` object:

* `aws_connection.login` contains the **Access Key ID**
* `aws_connection.password` contains the **Secret Access Key**

**MetastoreBackend Usage**

```
from airflow.decorators import dag
from airflow.secrets.metastore import MetastoreBackend


@dag(
    start_date=pendulum.now()
)
def load_data_to_redshift_dag():

    @task
    def load_task():    
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        logging.info(vars(aws_connection))

```

**Logging Output**

```
[2022-08-11, 16:16:20 UTC] {l2_e4_s3_to_redshift copy.py:21} INFO - {'_sa_instance_state': <sqlalchemy.orm.state.InstanceState object at 0x7f4342ca2e10>, 'id': 1, 'conn_type': 'aws', 'login': 'AKIA4QE4NTH3R7EBEANN', 'conn_id': 'aws_credentials', '_password': '***'}

```

# Using PostgresHook

The `PostgresHook` class is a superclass of the Airflow `DbApiHook`. When you instantiate the class, it creates an object that contains all the connection details for the Postgres database. It retrieves the details from the Postgres connection you created earlier in the Airflow UI.

<br data-md>

Just pass the connection id that you created in the Airflow UI.

```undefined
from airflow.providers.postgres.operators.postgres import PostgresOperator
. . .
        redshift_hook = PostgresHook("redshift")

```

Call `.run()` on the returned `PostgresHook`object to execute SQL statements.

```
       redhisft_hook.run("SELECT * FROM trips")
```

# Using PostgresOperator

The `PostgresOperator` class executes sql, and accepts the following parameters:

* a `postgres_conn_id`
* a `task_id`
* the `sql` statement
* optionally a `dag`

```undefined
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag

@dag(
    start_date=pendulum.now()
)
def load_data_to_redshift_dag():


. . .
    create_table_task=PostgresOperator(
        task_id="create_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )
. . .
    create_table_task >> copy_data

```

# Tip:

Notice the `PostgresOperator` **doesn't** have a `dag` parameter in the above example. That is because we used the `@dag` decorator on the dag function: