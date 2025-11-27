import pandas as pd
import sys
import numpy as np
import re
import pyodbc
from io import BytesIO

# --- FUNÇÕES AUXILIARES ---

def _get_sql_connection():
    try:
        conexao = pyodbc.connect(
            'Driver={ODBC Driver 11 for SQL Server};'
            'Server=servererpsql.livrariadavila.com.br;'
            'Database=ERIS_LIVRARIAVILA;'
            'UID=Leandro;'
            'PWD=Leandro@123;'
            'Encrypt=no;'
        )
        return conexao
    except Exception as e:
        print(f"Erro SQL: {e}", file=sys.stderr)
        return None

def _limpar_isbn(isbn_sujo):
    if pd.isna(isbn_sujo): return None
    isbn_str = str(isbn_sujo).replace('.0', '').strip()
    isbn_limpo = re.sub(r'[^0-9]', '', isbn_str)
    return isbn_limpo if len(isbn_limpo) >= 8 else None

def _normalizar_nome_filial_simples(nome_sujo):
    if pd.isna(nome_sujo): return "desconhecida"
    return str(nome_sujo).strip().lower()

def _processar_venda_com_skiprows(stream, col_quant_nome):
    venda = pd.read_excel(stream, header=None, skiprows=15)
    colunas_para_ler = {0: 'filial', 2: 'ISBN', 5: 'Preco_Venda_F', 6: 'Vl. Unit._venda_bruto', 7: col_quant_nome}
    colunas_existentes = [c for c in colunas_para_ler.keys() if c in venda.columns]
    venda = venda[colunas_existentes].rename(columns=colunas_para_ler)
    
    venda['ISBN'] = venda['ISBN'].apply(_limpar_isbn)
    venda['filial'] = venda['filial'].apply(_normalizar_nome_filial_simples)
    venda[col_quant_nome] = pd.to_numeric(venda[col_quant_nome], errors='coerce').fillna(0)
    venda = venda.dropna(subset=['ISBN', col_quant_nome])
    
    agg_dict = {col_quant_nome: 'sum'}
    if 'Vl. Unit._venda_bruto' in venda.columns:
        venda['Vl. Unit._venda_bruto'] = pd.to_numeric(venda['Vl. Unit._venda_bruto'], errors='coerce').fillna(0)
        agg_dict['Vl. Unit._venda_bruto'] = 'sum'
    if 'Preco_Venda_F' in venda.columns:
        venda['Preco_Venda_F'] = pd.to_numeric(venda['Preco_Venda_F'], errors='coerce').fillna(0)
        agg_dict['Preco_Venda_F'] = 'first'

    df_venda = venda.groupby(['filial', 'ISBN'], as_index=False).agg(agg_dict)
    if 'Vl. Unit._venda_bruto' in df_venda.columns:
        df_venda = df_venda.rename(columns={'Vl. Unit._venda_bruto': 'Vl. Unit._venda'})
    return df_venda

# --- FUNÇÕES SQL ---

def buscar_fornecedores_sql():
    consulta = """
        SELECT DISTINCT C.CODECLI, C.FANTASIA
        FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P
        LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C ON P.CODECLI = C.CODECLI
        INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO
        WHERE C.FANTASIA IS NOT NULL AND C.CODECLI IS NOT NULL
        ORDER BY C.FANTASIA
    """
    conexao = _get_sql_connection()
    if conexao is None: return []
    try:
        df = pd.read_sql(consulta, conexao)
        conexao.close()
        return df.to_dict('records')
    except:
        if conexao: conexao.close()
        return []

def buscar_pedidos_sql(fornecedor_id, status_id, data_ini, data_fim):
    conexao = _get_sql_connection()
    if conexao is None: return []
    if not status_id or not fornecedor_id or not data_ini or not data_fim: return []
    
    consulta = """
        SELECT DISTINCT P.PEDIDO, P.DT_PED, C2.FANTASIA AS FILIAL
        FROM ERIS_LIVRARIAVILA.DBO.PEDC_CAB P
        INNER JOIN ERIS_LIVRARIAVILA.DBO.PEDC_CAB_CONSIG PC ON P.PEDIDO = PC.PEDIDO
        LEFT JOIN ERIS_LIVRARIAVILA.DBO.CLIENTE C2 ON P.EMITENTE = C2.CODECLI 
        WHERE P.CODECLI = ? AND P.STATUS = ? AND P.DT_PED >= ? AND P.DT_PED <= ?
        AND PC.TIPO_ACERTO = 1
        ORDER BY P.DT_PED DESC, P.PEDIDO DESC
    """
    try:
        df = pd.read_sql(consulta, conexao, params=[fornecedor_id, status_id, data_ini, data_fim])
        conexao.close()
        df['DT_PED'] = pd.to_datetime(df['DT_PED']).dt.strftime('%d/%m/%Y')
        df['FILIAL'] = df['FILIAL'].fillna('')
        df['display_text'] = df['PEDIDO'] + ' - ' + df['FILIAL'] + ' (Data: ' + df['DT_PED'] + ')'
        return df.to_dict('records')
    except:
        if conexao: conexao.close()
        return []

def buscar_acerto_sql(pedidos_list):
    conexao = _get_sql_connection()
    if conexao is None or not pedidos_list: return pd.DataFrame()
    
    placeholders = ','.join('?' for _ in pedidos_list)
    try:
        consulta = f"""
        SELECT 
            C2.FANTASIA AS FILIAL, 
            PIT.DESCRICAO AS Titulo, 
            PRODUTO.COD_BARRA AS ISBN,
            PIT.QTT AS Quant, 
            PIT.PRECUNITTAB AS VlUnit, 
            ROUND(P.DESC_TOTAL, 2) AS Desconto,
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
        df = pd.read_sql(consulta, conexao, params=pedidos_list)
        conexao.close()
        return df
    except Exception as e:
        if conexao: conexao.close()
        print(f"Erro SQL Acerto: {e}")
        return pd.DataFrame()

def processar_acerto_sql(df_raw_sql):
    if df_raw_sql.empty: return pd.DataFrame(), "Sem Filial", "Sem Fornecedor"
    
    df = df_raw_sql.drop_duplicates(subset=['PROPOSTA', 'PRODCODE', 'Quant'])
    df['filial'] = df['FILIAL'].apply(_normalizar_nome_filial_simples)
    df['ISBN_limpo'] = df['ISBN'].apply(_limpar_isbn)
    df = df[df['ISBN_limpo'].notna()].copy()
    
    if df.empty: return pd.DataFrame(), "Sem Filial", "Sem Fornecedor"
    
    df['Desconto'] = pd.to_numeric(df['Desconto'], errors='coerce').fillna(0) / 100.0
    df['Quant'] = pd.to_numeric(df['Quant'], errors='coerce').fillna(0)
    
    if 'VlUnit' not in df.columns: df['VlUnit'] = 0
    df['VlUnit'] = pd.to_numeric(df['VlUnit'], errors='coerce').fillna(0)
    
    fornecedor_global = df['FORNECEDOR'].iloc[0] if 'FORNECEDOR' in df.columns and not df.empty else "Não Identificado"

    df_acerto = df.groupby(['ISBN_limpo', 'filial'], as_index=False).agg({
        'Quant': 'sum', 'Titulo': 'first', 'VlUnit': 'first',
        'Desconto': 'first'
    })
    df_acerto['fornecedor'] = fornecedor_global
    df_acerto = df_acerto.rename(columns={'ISBN_limpo': 'ISBN', 'VlUnit': 'Vl. Unit._acerto'})
    if 'Vl. Unit._acerto' not in df_acerto.columns: df_acerto['Vl. Unit._acerto'] = 0.0
    
    return df_acerto, "Múltiplas Filiais", fornecedor_global

def buscar_vendas_sql(data_ini, data_fim, fornecedor_id):
    conexao = _get_sql_connection()
    if conexao is None or not data_ini or not data_fim or not fornecedor_id: 
        return pd.DataFrame(columns=['filial', 'ISBN', 'Quant_venda', 'Vl. Unit._venda', 'Preco_Venda_F'])
    
    consulta = """
        SELECT SUBSTRING(f.FANTASIA, 1, 150) AS Filial, ISNULL(p.novo_isbn, p.cod_barra) AS ISBN,
        I.PRECUNITLIQ AS CAPA, round(i.QTT*i.PRECUNITLIQ,4) as Valor_Total, ISNULL(ROUND(i.QTT, 3), 0) AS Quantidade
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
    try:
        df_raw = pd.read_sql(consulta, conexao, params=[data_ini, data_fim, fornecedor_id])
        conexao.close()
        if df_raw.empty: return pd.DataFrame(columns=['filial', 'ISBN', 'Quant_venda', 'Vl. Unit._venda', 'Preco_Venda_F'])
        
        df_raw['filial'] = df_raw['Filial'].apply(_normalizar_nome_filial_simples)
        df_raw['ISBN'] = df_raw['ISBN'].apply(_limpar_isbn)
        df_raw = df_raw.dropna(subset=['ISBN'])
        df_raw['Quantidade'] = pd.to_numeric(df_raw['Quantidade'], errors='coerce').fillna(0)
        df_raw['Valor_Total'] = pd.to_numeric(df_raw['Valor_Total'], errors='coerce').fillna(0)
        df_raw['CAPA'] = pd.to_numeric(df_raw['CAPA'], errors='coerce').fillna(0)
        
        df_venda = df_raw.groupby(['filial', 'ISBN'], as_index=False).agg({
            'Quantidade': 'sum', 'Valor_Total': 'sum', 'CAPA': 'first'
        })
        return df_venda.rename(columns={'Quantidade': 'Quant_venda', 'Valor_Total': 'Vl. Unit._venda', 'CAPA': 'Preco_Venda_F'})
    except:
        if conexao: conexao.close()
        return pd.DataFrame(columns=['filial', 'ISBN', 'Quant_venda', 'Vl. Unit._venda', 'Preco_Venda_F'])

# --- FUNÇÕES EXCEL ---

def carregar_acerto(stream):
    try:
        dados_sem_cabecalho = pd.read_excel(stream, header=None)
        filial_bruta = dados_sem_cabecalho.iloc[0, 2]
        fornecedor = dados_sem_cabecalho.iloc[15, 1]
        filial = _normalizar_nome_filial_simples(filial_bruta)
        
        header_row_index = -1
        for i, row in dados_sem_cabecalho.iterrows():
            row_values = [str(x).strip() for x in row.values]
            if 'ISBN' in row_values and ('Quant' in row_values or 'Desc.' in row_values):
                header_row_index = i
                nomes_colunas = row_values
                break
        
        if header_row_index == -1: return pd.DataFrame(), "", ""
        
        acerto = pd.DataFrame(dados_sem_cabecalho.iloc[header_row_index + 1:].values, columns=nomes_colunas)
        
        col_map = {
            next((c for c in nomes_colunas if 'Titulo' in c), 'Titulo'): 'Titulo',
            next((c for c in nomes_colunas if 'ISBN' in c), 'ISBN'): 'ISBN',
            next((c for c in nomes_colunas if 'Quant' in c), 'Quant'): 'Quant',
            next((c for c in nomes_colunas if 'Vl. Unit.' in c), 'Vl. Unit.'): 'Vl. Unit.',
            next((c for c in nomes_colunas if 'Desc.' in c), 'Desconto'): 'Desconto'
        }
        acerto = acerto.rename(columns=col_map)
        acerto['ISBN_limpo'] = acerto['ISBN'].apply(_limpar_isbn)
        acerto = acerto[acerto['ISBN_limpo'].notna()].copy()
        
        if acerto.empty: return pd.DataFrame(), "", ""
        
        acerto['Desconto'] = pd.to_numeric(acerto['Desconto'], errors='coerce').fillna(0) / 100.0
        acerto['Quant'] = pd.to_numeric(acerto['Quant'], errors='coerce').fillna(0)
        acerto['Vl. Unit.'] = pd.to_numeric(acerto['Vl. Unit.'], errors='coerce').fillna(0)
        acerto['filial'] = filial
        acerto['fornecedor'] = fornecedor

        df_acerto = acerto.groupby(['ISBN_limpo', 'filial'], as_index=False).agg({
            'Quant': 'sum', 'Titulo': 'first', 'Vl. Unit.': 'first', 'Desconto': 'first', 'fornecedor': 'first'
        })
        return df_acerto.rename(columns={'ISBN_limpo': 'ISBN', 'Vl. Unit.': 'Vl. Unit._acerto'}), filial_bruta, fornecedor
    except: return pd.DataFrame(), "Erro", "Erro"

def carregar_venda(stream):
    try: return _processar_venda_com_skiprows(stream, 'Quant_venda')
    except: return pd.DataFrame(columns=['filial', 'ISBN', 'Quant_venda', 'Vl. Unit._venda', 'Preco_Venda_F'])

def carregar_venda_acao(stream):
    try:
        df = _processar_venda_com_skiprows(stream, 'Quant_acao')
        return df[['filial', 'ISBN', 'Quant_acao']] if not df.empty else pd.DataFrame(columns=['filial', 'ISBN', 'Quant_acao'])
    except: return pd.DataFrame(columns=['filial', 'ISBN', 'Quant_acao'])

def carregar_quebra_inventario(stream):
    try:
        try: df = pd.read_excel(stream, header=None)
        except: 
            stream.seek(0)
            df = pd.read_csv(stream, header=None, sep=',', encoding='latin1')
            
        filial = _normalizar_nome_filial_simples(str(df.iloc[0, 4]).strip())
        idx_header = -1
        for i, r in df.iterrows():
            if len(r) > 7 and str(r[7]).strip().upper() == 'ISBN':
                idx_header = i; break
        
        if idx_header == -1: return pd.DataFrame()
        
        df_d = df.iloc[idx_header+1:].copy()
        df_d = df_d.rename(columns={7: 'ISBN', 9: 'Contado', 10: 'Estoque'})
        df_d['ISBN'] = df_d['ISBN'].apply(_limpar_isbn)
        df_d = df_d.dropna(subset=['ISBN'])
        df_d['Quebra_Inv'] = pd.to_numeric(df_d['Estoque'], errors='coerce').fillna(0) - pd.to_numeric(df_d['Contado'], errors='coerce').fillna(0)
        df_d['filial'] = filial
        return df_d.groupby(['filial', 'ISBN'], as_index=False).agg({'Quebra_Inv': 'sum'})
    except: return pd.DataFrame(columns=['filial', 'ISBN', 'Quebra_Inv'])

# --- CÁLCULO CONSOLIDADO ---

def gerar_planilha_conferencia(df_acerto, df_venda, df_acao, isbns_promo, df_quebra):
    if not df_acerto.empty: df_acerto[['ISBN', 'filial']] = df_acerto[['ISBN', 'filial']].astype(str)
    if not df_venda.empty: df_venda[['ISBN', 'filial']] = df_venda[['ISBN', 'filial']].astype(str)
    if not df_acao.empty: df_acao[['ISBN', 'filial']] = df_acao[['ISBN', 'filial']].astype(str)
    if not df_quebra.empty: df_quebra[['ISBN', 'filial']] = df_quebra[['ISBN', 'filial']].astype(str)
    
    if df_acerto.empty: return pd.DataFrame()

    df = pd.merge(df_acerto, df_venda, on=['filial', 'ISBN'], how='left')
    df = pd.merge(df, df_acao, on=['filial', 'ISBN'], how='left')
    df = pd.merge(df, df_quebra, on=['filial', 'ISBN'], how='left')

    for c in ['Quant', 'Quant_venda', 'Quant_acao', 'Quebra_Inv', 'Vl. Unit._acerto', 'Desconto']:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    if isbns_promo:
        promo_set = {_limpar_isbn(i) for i in re.split(r'[\s,;\n]+', isbns_promo) if i.strip()}
        df['Item Promocional'] = df['ISBN'].apply(lambda x: 'Sim' if x in promo_set else 'Não')
    else: df['Item Promocional'] = 'Não'

    df['Divergência Qtd.'] = np.where(df['Quant'] > df['Quant_venda'], df['Quant'] - df['Quant_venda'], 0)
    df['Situação Qtd.'] = np.where(df['Divergência Qtd.'] > 0, 'Divergência', 'OK')
    df.loc[df['Item Promocional'] == 'Sim', 'Situação Qtd.'] = 'Ação'

    df['Preco_Venda_F'] = pd.to_numeric(df.get('Preco_Venda_F', 0), errors='coerce').fillna(0)
    df['Divergência Preço'] = df['Vl. Unit._acerto'] - df['Preco_Venda_F']
    df['Situação Preço'] = np.where(abs(df['Divergência Preço']) > 0.01, 'Divergência', 'OK')

    df['Vl. Unit. Liq. Acerto'] = df['Vl. Unit._acerto'] * (1 - df['Desconto'])
    df['Vlr. Liq. Qtd. Divergência'] = df['Divergência Qtd.'] * df['Vl. Unit. Liq. Acerto']

    if not df_quebra.empty:
        df['Calc_S'] = df[['Quebra_Inv', 'Quant']].min(axis=1)
        df['Calc_T'] = df['Calc_S']
        df['Div_Temp'] = df['Quant'] - df['Quant_venda']
        df['Calc_U'] = np.where(df['Div_Temp'] > df['Calc_T'], df['Quant'], df['Div_Temp'])
        df['Calc_V'] = df[['Calc_U', 'Calc_T']].min(axis=1)
        df['Quebra_Inv'] = np.where(df['Divergência Qtd.'] > 0, df['Calc_V'], 0).astype(int)
        df['Vlr. Quebra Liquida'] = df['Quebra_Inv'] * df['Vl. Unit. Liq. Acerto']
        df['Vlr. Quebra Bruta'] = df['Quebra_Inv'] * df['Vl. Unit._acerto']
    else:
        df['Quebra_Inv'] = 0
        df['Vlr. Quebra Liquida'] = 0
        df['Vlr. Quebra Bruta'] = 0

    df['Qtd. a Acertar'] = 0
    mask_acao = (df['Item Promocional'] == 'Sim') & (df['Quant_acao'] > 0)
    df.loc[mask_acao, 'Qtd. a Acertar'] = (df['Quant'] - df['Quant_acao']).clip(lower=0)
    df['Qtd. a Acertar'] = df['Qtd. a Acertar'].astype(int)
    
    df['Vlr. Liq. A Acertar'] = 0.0
    df['Qtd. Final'] = 0.0
    df['Titulo'] = df['Titulo'].fillna('Não Informado').astype(str)
    
    return df

def calcular_qtd_final(df):
    df_c = df.copy()
    cols = ['Quant', 'Quant_venda', 'Quant_acao', 'Qtd. a Acertar', 'Vl. Unit._acerto', 'Desconto', 'Divergência Qtd.']
    for c in cols: df_c[c] = pd.to_numeric(df_c.get(c, 0), errors='coerce').fillna(0)
    
    mask_div = df_c['Situação Qtd.'] == 'Divergência'
    mask_acao_edit = (df_c['Item Promocional'] == 'Sim') & (df_c['Quant_acao'] > 0)
    mask_acao_auto = (df_c['Item Promocional'] == 'Sim') & (df_c['Quant_acao'] == 0)
    mask_ok = (~mask_div) & (~mask_acao_edit) & (~mask_acao_auto)

    df_c.loc[mask_div, 'Qtd. a Acertar'] = df_c.loc[mask_div, 'Qtd. a Acertar'].clip(upper=df_c.loc[mask_div, 'Divergência Qtd.'])
    lim = (df_c.loc[mask_acao_edit, 'Quant'] - df_c.loc[mask_acao_edit, 'Quant_acao']).clip(lower=0)
    df_c.loc[mask_acao_edit, 'Qtd. a Acertar'] = df_c.loc[mask_acao_edit, 'Qtd. a Acertar'].clip(upper=lim)

    df_c.loc[mask_div, 'Qtd. Final'] = df_c['Quant_venda'] + df_c['Qtd. a Acertar']
    df_c.loc[mask_acao_edit, 'Qtd. Final'] = df_c['Quant_acao'] + df_c['Qtd. a Acertar']
    df_c.loc[mask_acao_auto, 'Qtd. Final'] = df_c['Quant']
    df_c.loc[mask_ok, 'Qtd. Final'] = df_c['Quant']
    
    df_c['Qtd. Final'] = df_c['Qtd. Final'].clip(lower=0).astype(int)
    df_c['Vlr. Liq. A Acertar'] = df_c['Qtd. a Acertar'] * df_c['Vl. Unit._acerto'] * (1 - df_c['Desconto'])
    return df_c

def gerar_resumo_consolidado(df, sum_venda):
    df = df.copy()
    df['Vl. Unit. Liq. Acerto'] = df['Vl. Unit._acerto'] * (1 - df['Desconto'])
    df['Vlr. Liq. A Acertar'] = df['Qtd. a Acertar'] * df['Vl. Unit. Liq. Acerto']
    df['Valor Bruto (Total Acertado)'] = df['Qtd. Final'] * df['Vl. Unit._acerto']
    df['Valor Líquido (Total Acertado)'] = df['Qtd. Final'] * df['Vl. Unit. Liq. Acerto']

    res = df.groupby('filial', as_index=False).agg({
        'Valor Líquido (Total Acertado)': 'sum',
        'Valor Bruto (Total Acertado)': 'sum',
        'Vlr. Liq. Qtd. Divergência': 'sum',
        'Vlr. Liq. A Acertar': 'sum'
    })
    
    if sum_venda is not None and not sum_venda.empty:
        res = pd.merge(res, sum_venda, on='filial', how='outer').fillna(0)
    else: res['Venda Bruta'] = 0.0

    res = res.rename(columns={'Vlr. Liq. Qtd. Divergência': 'Vlr. Divergente no Acerto (Total)', 'Vlr. Liq. A Acertar': 'Vlr. Líquido a Acertar (Manual)'})
    
    totais = {
        'TOTAL ESTIMADO DA DIVERGÊNCIA LÍQUIDA': df['Vlr. Liq. Qtd. Divergência'].sum(),
        'TOTAL LÍQUIDO A ACERTAR (MANUAL)': df['Vlr. Liq. A Acertar'].sum()
    }
    return res, totais