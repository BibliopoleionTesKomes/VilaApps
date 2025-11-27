import pyodbc
import pandas as pd
import warnings

warnings.filterwarnings('ignore', category=UserWarning)

CONN_STR = (
    'Driver={ODBC Driver 11 for SQL Server};'
    'Server=servererpsql.livrariadavila.com.br;'
    'Database=ERIS_LIVRARIAVILA;'
    'UID=Leandro;'
    'PWD=Leandro@123;'
    'Encrypt=no;'
)

def _executar_query(sql, params=None):
    try:
        conn = pyodbc.connect(CONN_STR)
        df = pd.read_sql(sql, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        print(f"Erro SQL: {e}")
        return pd.DataFrame()

# --- NOVA FUNÇÃO: DADOS DE CONTATO ---
def buscar_contato_fornecedor(cod_cli):
    """Busca telefone e email do fornecedor para contato."""
    sql = """
    SELECT
        CODECLI AS COD, 
        FANTASIA AS NOME,
        CGC_CPF AS CNPJ, 
        TELEFONE, 
        EMAIL
    FROM CLIENTE 
    WHERE CODECLI = ?
    """
    return _executar_query(sql, [cod_cli]).to_dict('records')

# --- NOVA FUNÇÃO: FILIAIS OFICIAIS (CATEGORIA 6) ---
def buscar_filiais_sql():
    """
    Busca apenas as filiais da CATEGORIA 6.
    Ordena por Código para que, se houver CNPJ duplicado, pegue a loja principal (menor código).
    """
    sql = """
    SELECT 
        CGC_CPF AS CNPJ, 
        FANTASIA AS Nome_Filial,
        CODECLI
    FROM CLIENTE 
    WHERE CATEGORIA = 6
    ORDER BY CODECLI ASC
    """
    df = _executar_query(sql)
    
    if not df.empty:
        df = df.drop_duplicates(subset=['CNPJ'], keep='first')
        df = df.drop(columns=['CODECLI'])
        
    return df

def buscar_dados_fornecedores():
    sql = """
    SELECT  
        F.CGC_CPF AS CNPJ,
        F.Fantasia AS Nome_Fantasia,
        condpag_desc AS Prazo,
        FORMAT(co.DATA_VENCIMENTO, 'dd') AS Dia_Acerto
    FROM consignacoes co
    INNER JOIN CLIENTE F ON F.CODECLI = co.CODG_FORNECEDOR
    INNER JOIN COND_PAG c ON c.CONDPAG_ID = co.CONDPAG_ID
    WHERE co.INDI_STATUS = 'A'
    """
    return _executar_query(sql).drop_duplicates(subset=['CNPJ'])

def buscar_itens_pedidos_lote(lista_pedidos, tipo_acerto_alvo=1):
    pedidos = list(set([p for p in lista_pedidos if p]))
    if not pedidos: return pd.DataFrame()
    placeholders = ','.join(['?'] * len(pedidos))
    
    sql = f"""
    SELECT
        PC.PEDIDO AS Numero_Pedido_Chave, PC.TIPO_ACERTO, C2.FANTASIA AS Filial, C.FANTASIA AS Fornecedor,
        P.DT_PED AS Data_Emissao, PRODUTO.COD_BARRA AS ISBN, PIT.DESCRICAO AS Titulo, PIT.QTT AS Quant,
        PIT.PRECUNITLIQ AS VlLiqUnit, (PIT.PRECUNITLIQ * PIT.QTT) AS Valor_Liquido,
        (PIT.PRECUNITTAB * PIT.QTT) AS Valor_Bruto
    FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.PEDC_ITEM PIT ON P.pedido = PIT.PEDIDO
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.PRODUTO ON PIT.PRODCODE = PRODUTO.PRODCODE
    INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C2 ON P.EMITENTE = C2.CODECLI
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C ON P.CODECLI = C.CODECLI
    WHERE PC.PEDIDO IN ({placeholders}) AND PC.TIPO_ACERTO = {tipo_acerto_alvo} AND P.STATUS = 1
    """
    df = _executar_query(sql, pedidos)
    if not df.empty: 
        df['Numero_Pedido_Chave'] = df['Numero_Pedido_Chave'].astype(str)
        for c in ['Quant', 'Valor_Liquido', 'Valor_Bruto']: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
    return df

def buscar_pedido_manual(numero_pedido, tipo_acerto_alvo=1):
    sql = """
    SELECT
        PC.PEDIDO AS Numero_Pedido, PC.TIPO_ACERTO, C2.FANTASIA AS Filial, C.FANTASIA AS Fornecedor,
        P.DT_PED AS Data_Emissao, PRODUTO.COD_BARRA AS ISBN, PIT.DESCRICAO AS Titulo, PIT.QTT AS Quant,
        PIT.PRECUNITLIQ AS VlLiqUnit, (PIT.PRECUNITLIQ * PIT.QTT) AS Valor_Liquido,
        (PIT.PRECUNITTAB * PIT.QTT) AS Valor_Bruto
    FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.PEDC_ITEM PIT ON P.pedido = PIT.PEDIDO
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.PRODUTO ON PIT.PRODCODE = PRODUTO.PRODCODE
    INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C2 ON P.EMITENTE = C2.CODECLI
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C ON P.CODECLI = C.CODECLI
    WHERE PC.PEDIDO = ? AND PC.TIPO_ACERTO = ? AND P.STATUS = 1
    """
    df = _executar_query(sql, [numero_pedido, tipo_acerto_alvo])
    if not df.empty:
        for c in ['Quant', 'Valor_Liquido', 'Valor_Bruto']: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
    return df

def listar_fornecedores(tipo_acerto_alvo=1):
    sql = "SELECT DISTINCT TOP 2000 C.CODECLI, C.FANTASIA FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO INNER JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C ON P.CODECLI = C.CODECLI WHERE PC.TIPO_ACERTO = ? AND P.STATUS = 1 ORDER BY C.FANTASIA"
    return _executar_query(sql, [tipo_acerto_alvo]).to_dict('records')

def listar_filiais_do_fornecedor(cod_cli, tipo_acerto_alvo=1):
    sql = "SELECT DISTINCT C2.CODECLI, C2.FANTASIA FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C2 ON P.EMITENTE = C2.CODECLI WHERE P.CODECLI = ? AND PC.TIPO_ACERTO = ? AND P.STATUS = 1 AND C2.CODECLI IS NOT NULL ORDER BY C2.FANTASIA"
    return _executar_query(sql, [cod_cli, tipo_acerto_alvo]).to_dict('records')

def listar_pedidos_do_fornecedor(cod_cli, cod_filial=None, data_ini=None, data_fim=None, tipo_acerto_alvo=1):
    sql = "SELECT TOP 500 P.PEDIDO, C2.FANTASIA AS Filial, P.DT_PED AS Data_Emissao FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C2 ON P.EMITENTE = C2.CODECLI WHERE P.CODECLI = ? AND PC.TIPO_ACERTO = ? AND P.STATUS = 1"
    params = [cod_cli, tipo_acerto_alvo]
    if cod_filial: sql += " AND P.EMITENTE = ?"; params.append(cod_filial)
    if data_ini: sql += " AND P.DT_PED >= ?"; params.append(data_ini)
    if data_fim: sql += " AND P.DT_PED <= ?"; params.append(data_fim)
    sql += " ORDER BY P.DT_PED DESC"
    return _executar_query(sql, params).to_dict('records')