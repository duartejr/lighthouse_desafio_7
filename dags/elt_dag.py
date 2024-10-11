from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow import DAG

# There args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["antonio.junior@indicium.tech"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "NorthwindELT",
    default_args=default_args,
    description="A ELT dag for the Northeind ECommerceData",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    dag.doc_md = """
    ELT Diária do banco de dados de commerce Northwind, começando em 2022-02-07
    """
    
    extract_postgres_task = BashOperator(
        task_id='extract_postgres',
        bash_command='isaa!!!',
    )
    
    extract_postgres_task.doc_md = dedent(
        """\
        ### Task Documentation
         
        Essa task extrai os dados do banco de dados postgres, parte de baixo do step 1 da imagem:
        
        ![img](https://user-images.githubusercontent.com/49417424/105993225-e2aefb00-6084-11eb-96af-3ec3716b151a.png)
        """
    )
    
    load_postgres_data_to_db_task = BashOperator(
        task_id="load_postgres",
        bash_command='echo "Loaded postgres data do db!"',
    )
    
    load_postgres_data_to_db_task.doc_md = dedent(
        """\
        #### Task Documentation
        
        Essa task faz load dos dados extraído do postgres para hd, load para o banco de dados
        da parte dos dados extraídos do postgres no step 2 da imagem:
        
        ![img](https://user-images.githubusercontent.com/49417424/105993225-e2aefb00-6084-11eb-96af-3ec3716b151a.png)
        
        """
    )
    
    load_csv_data_to_db_task = BashOperator(
        task_id="load_csv",
        bash_command='echo "Loaded csv data do db!"',
    )
    
    load_csv_data_to_db_task.doc_md = dedent(
        """\
    #### Task Documentation

    Essa task faz load dos dados csv, load para o banco de dados
    da parte dos dados extraidos do csv no step 2 da imagem: 

    ![img](https://user-images.githubusercontent.com/49417424/105993225-e2aefb00-6084-11eb-96af-3ec3716b151a.png)

    """
    )
    
    run_sales_query_task = BashOperator(
        task_id="run_sales_query_task",
        bash_command='echo "we sold alot!!"',
    )
    
    run_sales_query_task.doc_md = dedent(
        """\
    #### Task Documentation
        Query em cima do banco consolidado, pegando o valor das vendas para o dia
    """
    )
    
    extract_postgres_task >> load_postgres_data_to_db_task >> Label("Resultado Consolidado") >> run_sales_query_task
    load_csv_data_to_db_task >> Label("Resultado Consolidado") >> run_sales_query_task