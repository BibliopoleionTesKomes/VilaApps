# --- IMPORTAÇÕES ---
import json
import pandas as pd
import re
from datetime import datetime

# Importações do projeto
from app.repository.geral_repo import buscar_filiais, buscar_dados_fornecedores, buscar_itens_pedidos_lote
from app.services.cache_service import CACHE_ACERTO, CACHE_DEVOLUCAO, CACHE_GERAL
from config import Config

STATUS_GLOBAL = {'atual': 0, 'total': 0, 'status': 'parado', 'msg': ''}

def atualizar_progresso(atual, total):
    STATUS_GLOBAL['atual'] = atual
    STATUS_GLOBAL['total'] = total
    STATUS_GLOBAL['status'] = 'rodando'

def resetar_progresso():
    STATUS_GLOBAL['atual'] = 0
    STATUS_GLOBAL['total'] = 0
    STATUS_GLOBAL['status'] = 'iniciando'
    STATUS_GLOBAL['msg'] = ''

# --- FUNÇÕES AUXILIARES ---

def limpar_cnpj(valor):
    if not valor: return ""
    return re.sub(r'\D', '', str(valor)).zfill(14) 

def formatar_moeda(val):
    try: return f"R$ {float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return val

def gerar_resumo_divergencia(nota):
    itens_xml = nota.get('Itens', [])
    itens_erp = nota.get('Itens_ERP', [])
    
    map_xml = {str(i.get('ISBN', '')).strip(): i for i in itens_xml}
    map_erp = {str(i.get('ISBN', '')).strip(): i for i in itens_erp}
    
    todos_isbns = set(map_xml.keys()) | set(map_erp.keys())
    c_preco = 0
    c_qtd = 0

    for isbn in todos_isbns:
        x = map_xml.get(isbn)
        e = map_erp.get(isbn)
        
        if x and e:
            qtd_x = float(x.get('Quantidade', 0))
            qtd_e = float(e.get('Quant', 0))
            if qtd_x != qtd_e: c_qtd += 1
            
            val_liq_total_x = float(x.get('Valor_Liquido', 0))
            val_unit_x = val_liq_total_x / qtd_x if qtd_x > 0 else 0
            val_unit_e = float(e.get('VlLiqUnit', 0))
            
            if abs(val_unit_x - val_unit_e) > 0.01: c_preco += 1

    msgs = []
    if c_qtd > 0: msgs.append(f"Qtde Diferente ({c_qtd}x)")
    if c_preco > 0: msgs.append(f"Preço Diferente ({c_preco}x)")
    
    if not msgs:
        if not itens_xml: return "XML Vazio"
        return "" 
    return " | ".join(msgs)

# --- TAREFA PRINCIPAL (THREAD) ---

def tarefa_background(modulo, app_config):
    """
    Função principal que é executada em segundo plano.
    """
    caminho_xml = Config.CAMINHO_XML_PADRAO
    
    # DEBUG: Imprime no terminal para sabermos onde ele está a procurar
    print(f"--- INICIANDO PROCESSAMENTO BACKEND ---")
    print(f"Pasta Alvo: {caminho_xml}")
    
    if modulo == 'devolucao':
        cfops = ['5917', '6917']
        tipo_pedido = 4
        arquivo_cache = CACHE_DEVOLUCAO
    elif modulo == 'acerto':
        cfops = app_config.get('CFOPS_PADRAO')
        tipo_pedido = 1
        arquivo_cache = CACHE_ACERTO
    else: 
        cfops = app_config.get('CFOPS_PADRAO')
        tipo_pedido = 1
        arquivo_cache = CACHE_GERAL

    STATUS_GLOBAL['msg'] = 'Lendo arquivos XML...'
    
    from app.services.xml_service import processar_pasta_xml_thread_safe 
    
    pastas_ign = app_config.get('PASTAS_IGNORADAS', [])
    df_xml, msg = processar_pasta_xml_thread_safe(caminho_xml, cfops, atualizar_progresso, pastas_ign)
    
    # --- CORREÇÃO CRÍTICA AQUI ---
    # Se não encontrou XMLs, imprimimos o aviso mas CONTINUAMOS.
    # Isso garante que lá no final do código ele salve um arquivo vazio,
    # limpando os dados antigos da tela.
    if df_xml.empty:
        print(f"AVISO BACKEND: Nenhum XML encontrado. Motivo: {msg}")
        STATUS_GLOBAL['msg'] = msg or "Nenhum XML encontrado"
        # NÃO FAZEMOS MAIS 'return' AQUI. O CÓDIGO SEGUE PARA LIMPAR O ARQUIVO.

    STATUS_GLOBAL['msg'] = 'Cruzando dados com ERP...'
    
    # 2. Cruzamento com Lojas (Filiais)
    if not df_xml.empty and 'CNPJ_Destinatario' in df_xml.columns:
        df_xml['KEY_CNPJ'] = df_xml['CNPJ_Destinatario'].apply(limpar_cnpj)
        
        try:
            df_lojas_erp = buscar_filiais() 
            if not df_lojas_erp.empty:
                df_lojas_erp['KEY_CNPJ'] = df_lojas_erp['CNPJ'].apply(limpar_cnpj)
                df_merged = pd.merge(df_xml, df_lojas_erp[['KEY_CNPJ', 'Nome_Filial']], on='KEY_CNPJ', how='left')
                df_merged['Filial'] = df_merged['Nome_Filial'].fillna(df_merged['Nome_Destinatario'])
                df_merged['Filial'] = df_merged['Filial'].fillna("Filial Não Identificada")
                df_xml = df_merged.drop(columns=['Nome_Filial'])
            else:
                df_xml['Filial'] = df_xml.get('Nome_Destinatario', 'Sem Nome')
        except Exception as e:
            print(f"Erro ao buscar filiais: {e}")
            df_xml['Filial'] = df_xml.get('Nome_Destinatario', 'Sem Nome')
    elif not df_xml.empty:
        df_xml['Filial'] = 'Sem Destinatário'

    # 3. Cruzamento com Fornecedores
    df_final = df_xml.copy()
    if not df_xml.empty:
        try:
            df_forn = buscar_dados_fornecedores()
            if not df_forn.empty:
                df_xml['KEY_EMIT'] = df_xml['CNPJ_Emitente'].apply(limpar_cnpj)
                df_forn['KEY_EMIT'] = df_forn['CNPJ'].apply(limpar_cnpj)
                colunas_conflitantes = ['Nome_Fantasia', 'Prazo', 'Dia_Acerto']
                df_xml = df_xml.drop(columns=[c for c in colunas_conflitantes if c in df_xml.columns], errors='ignore')
                df_final = pd.merge(df_xml, df_forn, on='KEY_EMIT', how='left')
        except Exception as e:
            print(f"Erro ao buscar fornecedores: {e}")

    cols = ['Nome_Fantasia', 'Filial', 'Prazo', 'Dia_Acerto']
    for c in cols:
        if c not in df_final.columns: df_final[c] = '-'
        else: df_final[c] = df_final[c].fillna('-')
        
    df_final = df_final.fillna("")
    
    lista = df_final.to_dict('records')
    
    # 4. Busca de Pedidos Vinculados no ERP
    pedidos = [n.get('Numero_Pedido') for n in lista if n.get('Numero_Pedido')]
    
    df_itens_erp = pd.DataFrame()
    if pedidos:
        STATUS_GLOBAL['msg'] = f'Buscando {len(pedidos)} pedidos no Banco...'
        try:
            df_itens_erp = buscar_itens_pedidos_lote(pedidos, tipo_acerto_alvo=tipo_pedido)
            if not df_itens_erp.empty:
                for col in df_itens_erp.select_dtypes(include=['datetime', 'datetimetz']).columns:
                    df_itens_erp[col] = df_itens_erp[col].astype(str)
        except Exception as e:
            print(f"Erro ao buscar pedidos no lote: {e}")

    STATUS_GLOBAL['msg'] = 'Finalizando análises...'
    
    # 5. Cruzamento Final Item a Item
    for nota in lista:
        for item in nota.get('Itens', []):
            item.setdefault('Titulo', item.get('xProd', 'Produto Sem Nome'))
            if 'Valor_Liquido' not in item:
                try: item['Valor_Liquido'] = float(item.get('vProd', 0))
                except: item['Valor_Liquido'] = 0.0
            
            if 'Quantidade' in item:
                try: item['Quantidade'] = float(item['Quantidade'])
                except: item['Quantidade'] = 0.0
                
            if 'Valor_Unitario' not in item or item['Valor_Unitario'] == 0:
                qtd = item.get('Quantidade', 0)
                if qtd > 0: item['Valor_Unitario'] = item.get('Valor_Liquido', 0) / qtd
                else: item['Valor_Unitario'] = 0.0

        ped = str(nota.get('Numero_Pedido', ''))
        nota['Itens_ERP'] = []
        
        if ped and not df_itens_erp.empty:
            itens = df_itens_erp[df_itens_erp['Numero_Pedido_Chave'] == ped]
            if not itens.empty: nota['Itens_ERP'] = itens.to_dict('records')
        
        nota['Divergencia_Resumo'] = gerar_resumo_divergencia(nota)
        if 'Valor_Total' in nota: nota['Valor_Total'] = formatar_moeda(nota['Valor_Total'])

    # 6. Salva no Disco (Cache)
    # IMPORTANTE: Se a lista estiver vazia, ele vai salvar vazio, limpando o cache antigo.
    ts = datetime.now().strftime("%d/%m/%Y às %H:%M")
    try:
        print(f"Salvando {len(lista)} registros em: {arquivo_cache}")
        with open(arquivo_cache, 'w', encoding='utf-8') as f:
            json.dump({"timestamp": ts, "dados": lista}, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"ERRO AO SALVAR CACHE: {e}")

    # Avisa que acabou
    if not lista:
        STATUS_GLOBAL['status'] = 'concluido_vazio' # Status especial para avisar que limpou
        STATUS_GLOBAL['msg'] = 'Nenhum arquivo encontrado na pasta.'
    else:
        STATUS_GLOBAL['status'] = 'concluido'
        STATUS_GLOBAL['msg'] = 'Concluído!'