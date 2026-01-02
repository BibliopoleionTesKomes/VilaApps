from app.database import execute_query
import pandas as pd

def buscar_filiais():
    sql = "SELECT CGC_CPF AS CNPJ, FANTASIA AS Nome_Filial, CODECLI FROM CLIENTE WHERE CATEGORIA = 6 ORDER BY CODECLI ASC"
    df = execute_query(sql)
    if not df.empty:
        df = df.drop_duplicates(subset=['CNPJ'], keep='first')
        df = df.drop(columns=['CODECLI'])
    return df

def listar_fornecedores(tipo_acerto=1):
    sql = """
    SELECT DISTINCT TOP 2000 C.CODECLI, C.FANTASIA 
    FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P 
    INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO 
    INNER JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C ON P.CODECLI = C.CODECLI 
    WHERE PC.TIPO_ACERTO = ? AND P.STATUS IN (1, 3) 
    ORDER BY C.FANTASIA
    """
    return execute_query(sql, [tipo_acerto]).to_dict('records')

def listar_filiais_do_fornecedor(cod_cli, tipo_acerto=1):
    sql = """
    SELECT DISTINCT C2.CODECLI, C2.FANTASIA 
    FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P 
    INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO 
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C2 ON P.EMITENTE = C2.CODECLI 
    WHERE P.CODECLI = ? AND PC.TIPO_ACERTO = ? AND P.STATUS IN (1, 3) AND C2.CODECLI IS NOT NULL 
    ORDER BY C2.FANTASIA
    """
    return execute_query(sql, [cod_cli, tipo_acerto]).to_dict('records')

def listar_pedidos_do_fornecedor(cod_cli, cod_filial=None, data_ini=None, data_fim=None, tipo_acerto=1):
    sql = """
    SELECT TOP 500 P.PEDIDO, C2.FANTASIA AS Filial, P.DT_PED AS Data_Emissao,
    (SELECT SUM(QTT * PRECUNITLIQ) FROM ERIS_LIVRARIAVILA.DBO.PEDC_ITEM WHERE PEDIDO = P.PEDIDO) AS Valor_Total
    FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P 
    INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO  
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C2 ON P.EMITENTE = C2.CODECLI 
    WHERE P.CODECLI = ? AND PC.TIPO_ACERTO = ? AND P.STATUS IN (1, 3)
    """
    params = [cod_cli, tipo_acerto]
    if cod_filial: sql += " AND P.EMITENTE = ?"; params.append(cod_filial)
    if data_ini: sql += " AND P.DT_PED >= ?"; params.append(data_ini)
    if data_fim: sql += " AND P.DT_PED <= ?"; params.append(data_fim)
    sql += " ORDER BY P.DT_PED DESC"
    
    df = execute_query(sql, params)
    if not df.empty:
        # Garante string para o JSON
        df['Data_Emissao'] = pd.to_datetime(df['Data_Emissao']).astype(str)
        return df.to_dict('records')
    return []

def buscar_pedido_manual(numero_pedido, tipo_acerto=1):
    sql = """
    SELECT PC.PEDIDO AS Numero_Pedido, PC.TIPO_ACERTO, C2.FANTASIA AS Filial, C.FANTASIA AS Fornecedor,
    P.DT_PED AS Data_Emissao, PRODUTO.COD_BARRA AS ISBN, PIT.DESCRICAO AS Titulo, PIT.QTT AS Quant,
    PIT.PRECUNITLIQ AS VlLiqUnit, (PIT.PRECUNITLIQ * PIT.QTT) AS Valor_Liquido, (PIT.PRECUNITTAB * PIT.QTT) AS Valor_Bruto
    FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.PEDC_ITEM PIT ON P.pedido = PIT.PEDIDO
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.PRODUTO ON PIT.PRODCODE = PRODUTO.PRODCODE
    INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C2 ON P.EMITENTE = C2.CODECLI
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C ON P.CODECLI = C.CODECLI
    WHERE PC.PEDIDO = ? AND PC.TIPO_ACERTO = ? AND P.STATUS = 1
    """
    df = execute_query(sql, [numero_pedido, tipo_acerto])
    if not df.empty:
        for c in ['Quant', 'Valor_Liquido', 'Valor_Bruto', 'VlLiqUnit']: 
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
        return df.to_dict('records') # Retorna lista de dicts
    return []

def buscar_itens_pedidos_lote(lista_pedidos, tipo_acerto_alvo=1):
    pedidos = list(set([str(p) for p in lista_pedidos if p]))
    if not pedidos: return pd.DataFrame()
    
    placeholders = ','.join(['?'] * len(pedidos))
    sql = f"""
    SELECT PC.PEDIDO AS Numero_Pedido_Chave, PC.TIPO_ACERTO, C2.FANTASIA AS Filial, C.FANTASIA AS Fornecedor,
    P.DT_PED AS Data_Emissao, PRODUTO.COD_BARRA AS ISBN, PIT.DESCRICAO AS Titulo, PIT.QTT AS Quant,
    PIT.PRECUNITLIQ AS VlLiqUnit, (PIT.PRECUNITLIQ * PIT.QTT) AS Valor_Liquido, (PIT.PRECUNITTAB * PIT.QTT) AS Valor_Bruto
    FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.PEDC_ITEM PIT ON P.pedido = PIT.PEDIDO
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.PRODUTO ON PIT.PRODCODE = PRODUTO.PRODCODE
    INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C2 ON P.EMITENTE = C2.CODECLI
    LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C ON P.CODECLI = C.CODECLI
    WHERE PC.PEDIDO IN ({placeholders}) AND PC.TIPO_ACERTO = {tipo_acerto_alvo} AND P.STATUS = 1
    """
    df = execute_query(sql, pedidos)
    if not df.empty: 
        df['Numero_Pedido_Chave'] = df['Numero_Pedido_Chave'].astype(str)
        for c in ['Quant', 'Valor_Liquido', 'Valor_Bruto', 'VlLiqUnit']: 
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
    return df

def buscar_dados_fornecedores():
    sql = """
    SELECT F.CGC_CPF AS CNPJ, F.Fantasia AS Nome_Fantasia, condpag_desc AS Prazo, FORMAT(co.DATA_VENCIMENTO, 'dd') AS Dia_Acerto
    FROM consignacoes co
    INNER JOIN CLIENTE F ON F.CODECLI = co.CODG_FORNECEDOR
    INNER JOIN COND_PAG c ON c.CONDPAG_ID = co.CONDPAG_ID
    WHERE co.INDI_STATUS = 'A'
    """
    df = execute_query(sql)
    if not df.empty: return df.drop_duplicates(subset=['CNPJ'])
    return df