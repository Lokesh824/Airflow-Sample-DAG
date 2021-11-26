try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print("ALL DAG modules are ok...")

except Exception as e:
    print("Error {}".format(e))

def first_function_execute(*args, **kwargs):
    inputpram = kwargs.get("name", "No Name passed")
    print("Hello World")
    # setting the values here, so it can be used later
    kwargs["ti"].xcom_push(key="username", value="the user name passed from function ->"+inputpram)

def second_function_execute(*args, **kwargs):
    instance = kwargs.get("ti").xcom_pull(key="username")
    print("I am in second function now and the value passed is ->", instance)

with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retires": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021, 1, 1),
    },
    catchup=False
) as f:
    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name":"Lokesh Kumar"}
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True,
    )
# since the first function is dependent on the second function so writing arrow op.
first_function_execute >> second_function_execute