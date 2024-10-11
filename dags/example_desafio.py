from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open(f'count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open(f"final_output.txt","w") as f:
        f.write(base64_message)
    
    return None

## Do not change the code above this line-----------------------##

def get_order():
    # Conectando ao banco de dados SQLite
    conn = sqlite3.connect(Variable.get('DB_PATH'))

    # Executar a consulta SQL para pegar todos os dados da tabela 'Order'
    df = pd.read_sql_query("SELECT * FROM `Order`", conn)

    # Exportar os dados para um arquivo CSV
    df.to_csv(f"output_orders.csv", index=False, encoding='utf-8')

    # Fechar a conexão com o banco de dados
    conn.close()

def count_quantity():
    # Conectando ao banco de dados SQLite
    conn = sqlite3.connect(Variable.get('DB_PATH'))

    # Executar a consulta SQL para pegar todos os dados da tabela 'Order'
    df_orders_details = pd.read_sql_query("SELECT * FROM `OrderDetail`", conn)
    
    df_orders = pd.read_csv(f"output_orders.csv")
    
    # Garantindo que os indexes terão o mesmo tipo de dado
    df_orders['Id'] = df_orders['Id'].astype(str)
    df_orders_details['OrderId'] = df_orders_details['OrderId'].astype(str)
    
    # Merging dataframes
    df = pd.merge(df_orders, df_orders_details, how='inner', left_on='Id', right_on='OrderId')
    
    # Seleção de produtos com destino Rio de Janeiro
    df_rio = df.query("ShipCity == 'Rio de Janeiro'")
    
    # Calculo da quantidade vendida
    quantity = str(df_rio['Quantity'].sum())

    with open(f"count.txt", "w") as f:
        f.write(quantity)

    # Fechar a conexão com o banco de dados
    conn.close()

    
with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
   
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )
    
    export_orders = PythonOperator(
        task_id='export_orders',
        python_callable=get_order,
        provide_context=True
    )
    
    calc_quantity_rio = PythonOperator(
        task_id="calc_quantity_rio",
        python_callable=count_quantity,
        provide_context=True
    )
    
    export_orders >> calc_quantity_rio >> export_final_output
