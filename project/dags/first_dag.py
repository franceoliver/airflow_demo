try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute(**context):
    print("first_function_execute   ")
    data = [{"name":"Oliver","title":"Data Engineer"}, { "name":"Tori","title":"Marketing Manger"},]
    df = pd.DataFrame(data=data)
    ## Passing df from first function to second function
    context['ti'].xcom_push(key='mykey', value=df)

def second_function_execute(**context):
    ## Getting df from first function
    instance = context.get("ti").xcom_pull(key="mykey")
    print('@'*66)
    print(instance.head())
    print('@' * 66)


with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "start_date": datetime(2021, 1, 1),
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        catchup=False) as f:

    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name": "Oliver France"}
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True,
    )

first_function_execute >> second_function_execute