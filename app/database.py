import pyodbc
from flask import current_app
import pandas as pd
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

def get_connection():
    """
    Cria uma conexão com o banco usando a string definida no config.py
    """
    try:
        conn_str = current_app.config['SQL_CONNECTION_STRING']
        return pyodbc.connect(conn_str)
    except Exception as e:
        print(f"CRITICAL: Erro ao conectar no banco: {e}")
        return None

def execute_query(sql, params=None):
    """
    Executa uma query e retorna um DataFrame pandas.
    Esta é a função que o resto do sistema está a tentar importar e não encontra.
    """
    conn = get_connection()
    
    if conn is None:
        return pd.DataFrame()
    
    try:
        if params:
            df = pd.read_sql(sql, conn, params=params)
        else:
            df = pd.read_sql(sql, conn)
        return df
        
    except Exception as e:
        print(f"Erro na execução da query: {e}")
        return pd.DataFrame()
        
    finally:
        try:
            conn.close()
        except:
            pass