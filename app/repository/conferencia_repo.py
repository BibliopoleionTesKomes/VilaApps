# --- IMPORTAÇÕES ---
# Importa a função genérica de execução de SQL (que gerencia a conexão)
from app.database import execute_query
# Pandas: Essencial para receber os dados do SQL já em formato de tabela
import pandas as pd
import sys

def buscar_pedidos_para_conferencia(fornecedor_id, status_id, data_ini, data_fim):
    """
    Busca os pedidos (propostas) disponíveis para conferência com base nos filtros.
    Esta função alimenta a lista de seleção na tela inicial.
    """
    # SQL: Seleciona Pedidos de Consignação (TIPO_ACERTO = 1)
    # Faz JOINs para trazer o nome da Filial (FANTASIA) e data formatada
    sql = """
        SELECT DISTINCT P.PEDIDO, P.DT_PED, C2.FANTASIA AS FILIAL
        FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P
        INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO
        LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C2 ON P.EMITENTE = C2.CODECLI 
        WHERE P.CODECLI = ? AND P.STATUS = ? AND P.DT_PED >= ? AND P.DT_PED <= ?
        AND PC.TIPO_ACERTO = 1
        ORDER BY P.DT_PED DESC, P.PEDIDO DESC
    """
    
    # Executa a query passando os parâmetros de forma segura (evita SQL Injection)
    df = execute_query(sql, [fornecedor_id, status_id, data_ini, data_fim])
    
    # Se encontrou dados, faz um tratamento estético antes de enviar para o site
    if not df.empty:
        # Formata a data para o padrão brasileiro (DD/MM/AAAA)
        df['DT_PED'] = pd.to_datetime(df['DT_PED']).dt.strftime('%d/%m/%Y')
        # Preenche filiais vazias para não quebrar o layout
        df['FILIAL'] = df['FILIAL'].fillna('')
        # Retorna lista de dicionários (formato fácil para o HTML ler)
        return df.to_dict('records')
        
    return []

def buscar_acerto_sql_repo(pedidos_list):
    """
    Busca os ITENS detalhados dos pedidos selecionados pelo usuário.
    Esta consulta traz ISBN, Quantidade, Valor Unitário, etc.
    """
    if not pedidos_list: return pd.DataFrame()
    
    # Cria uma string de '?, ?, ?' baseada na quantidade de pedidos selecionados
    # Necessário para a cláusula 'IN' do SQL funcionar com parâmetros dinâmicos
    placeholders = ','.join('?' for _ in pedidos_list)
    
    sql = f"""
        SELECT 
            C2.FANTASIA AS FILIAL, 
            PIT.DESCRICAO AS Titulo, 
            PRODUTO.COD_BARRA AS ISBN,
            PIT.QTT AS Quant, 
            PIT.PRECUNITTAB AS VlUnit, 
            PIT.PRECUNITLIQ AS VlLiqItem,
            ROUND(P.DESC_TOTAL, 2) AS DescontoHeader,
            PIT.PRECUNITLIQ AS VlLiq, 
            (PIT.PRECUNITLIQ * PIT.QTT) AS TotalLiq,
            PC.PEDIDO AS PROPOSTA, 
            PIT.PRODCODE, 
            C.FANTASIA AS FORNECEDOR
        FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P
        LEFT JOIN ERIS_LIVRARIAVILA.DBO.PEDC_ITEM PIT ON P.PEDIDO = PIT.PEDIDO
        LEFT JOIN ERIS_LIVRARIAVILA.DBO.PRODUTO ON PIT.PRODCODE = PRODUTO.PRODCODE
        INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO
        LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C ON P.CODECLI = C.CODECLI 
        LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C2 ON P.EMITENTE = C2.CODECLI 
        WHERE PC.PEDIDO IN ({placeholders}) AND PC.TIPO_ACERTO = 1 
    """
    
    return execute_query(sql, pedidos_list)

def buscar_vendas_sql_repo(data_ini, data_fim, fornecedor_id):
    """
    Busca o relatório de VENDAS no período para cruzar com o Acerto.
    Essencial para o cálculo da divergência (O que deveria ser devolvido vs O que foi vendido).
    """
    sql = """
        SELECT SUBSTRING(f.FANTASIA, 1, 150) AS Filial, ISNULL(p.novo_isbn, p.cod_barra) AS ISBN,
        round(i.QTT*i.PRECUNITLIQ,4) + isnull(i.VALOR_IPI,0) + isnull(i.VL_ICMS_ST,0) - isnull(i.VL_ITEM_DESCONTO,0) + isnull(i.OUTRASDESPESAS_ACESSORIOS,0) + isnull(i.VL_FRETEXITEM,0) as Valor_Total, ISNULL(ROUND(i.QTT, 3), 0) AS Quantidade
        FROM ERIS_LIVRARIAVILA.dbo.NF_CAB N
        LEFT JOIN ERIS_LIVRARIAVILA.dbo.NF_ITEM I ON N.NF = I.NF AND N.EMITENTE = i.EMITENTE
        LEFT JOIN ERIS_LIVRARIAVILA.dbo.NATOPER NTOP ON N.NATUREZA_ID = NTOP.NATUREZA_ID
        LEFT JOIN ERIS_LIVRARIAVILA.dbo.CLIENTE f ON n.EMITENTE = f.CODECLI
        LEFT JOIN ERIS_LIVRARIAVILA.dbo.PRODUTO p ON i.PRODCODE = p.PRODCODE
        LEFT JOIN ERIS_LIVRARIAVILA.dbo.PROD_LINHA pl ON p.LINPROD_ID = pl.LINPROD_ID
        LEFT JOIN ERIS_LIVRARIAVILA.dbo.CLIENTE Forn ON pl.CODECLI = Forn.CODECLI
        WHERE N.STATUS = 0 AND N.TIPOPAG <> 4 AND N.TIPO_NF = 0 AND NTOP.TIPONATUREZA = 1 
        AND ISNULL(N.IS_NF_COMPLEMENTAR, '-1') NOT IN ('1')
        AND N.DT_FAT >= ? AND N.DT_FAT <= ? AND ISNULL(forn.codecli, 0) = ?
    """
    
    df_raw = execute_query(sql, [data_ini, data_fim, fornecedor_id])
    
    # Tratamento de erro importante:
    # Se não houver vendas, retorna um DataFrame vazio MAS com as colunas certas.
    # Isso evita erros de "KeyError" no serviço de cálculo quando ele tentar acessar essas colunas.
    if df_raw.empty: 
        return pd.DataFrame(columns=['filial', 'ISBN', 'Quant_venda', 'Vl. Unit._venda', 'Preco_Venda_F'])
    
    return df_raw