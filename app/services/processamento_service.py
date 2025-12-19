# --- IMPORTAÇÕES ---
# Threading: Permite rodar esta tarefa em paralelo ("em segundo plano") 
# para que o site não fique travado esperando o processamento terminar.
import threading
import os
import json      # Para salvar o resultado final em arquivo texto estruturado
import pandas as pd # Para manipular tabelas de dados de forma eficiente
import re        # Expressões Regulares (para limpar texto, como CNPJ)
from datetime import datetime

# Importações do próprio projeto
from flask import current_app
# Importamos a função que lê a pasta física de XMLs (comentada mais abaixo sobre a versão thread_safe)
from app.services.xml_service import processar_pasta_xml
# Repositórios para buscar dados no SQL Server
from app.repository.geral_repo import buscar_filiais, buscar_dados_fornecedores, buscar_itens_pedidos_lote
# Caminhos onde os arquivos JSON serão salvos
from app.services.cache_service import CACHE_ACERTO, CACHE_DEVOLUCAO, CACHE_GERAL

# --- VARIÁVEL GLOBAL DE STATUS ---
# Como a Thread roda separada do site, usamos esta variável global para 
# que o site possa perguntar "quantos % já foi?" (Polling).
# É como um quadro de avisos compartilhado.
STATUS_GLOBAL = {'atual': 0, 'total': 0, 'status': 'parado', 'msg': ''}

def atualizar_progresso(atual, total):
    """Atualiza o quadro de avisos com o progresso atual."""
    STATUS_GLOBAL['atual'] = atual
    STATUS_GLOBAL['total'] = total
    STATUS_GLOBAL['status'] = 'rodando'

def resetar_progresso():
    """Limpa o quadro de avisos antes de começar uma nova tarefa."""
    STATUS_GLOBAL['atual'] = 0
    STATUS_GLOBAL['total'] = 0
    STATUS_GLOBAL['status'] = 'iniciando'
    STATUS_GLOBAL['msg'] = ''

# --- FUNÇÕES AUXILIARES ---

def limpar_cnpj(valor):
    """
    Remove pontos, traços e barras do CNPJ e garante que tenha 14 dígitos.
    Isso é vital para conseguir cruzar dados do XML com dados do Banco.
    """
    if not valor: return ""
    return re.sub(r'\D', '', str(valor)).zfill(14) 

def formatar_moeda(val):
    """Formata um número (float) para o padrão de moeda brasileiro (R$ 1.000,00)."""
    try: return f"R$ {float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return val

def gerar_resumo_divergencia(nota):
    """
    Lógica inteligente que compara os itens lidos do XML com os itens buscados no ERP.
    Retorna uma string simples resumindo o problema (ex: "Qtde Diferente (2x)").
    """
    itens_xml = nota.get('Itens', [])
    itens_erp = nota.get('Itens_ERP', [])
    
    # Cria "mapas" (dicionários) usando o ISBN como chave para busca rápida
    map_xml = {str(i.get('ISBN', '')).strip(): i for i in itens_xml}
    map_erp = {str(i.get('ISBN', '')).strip(): i for i in itens_erp}
    
    # Pega todos os ISBNs únicos que existem em qualquer um dos dois lados
    todos_isbns = set(map_xml.keys()) | set(map_erp.keys())
    c_preco = 0
    c_qtd = 0

    for isbn in todos_isbns:
        x = map_xml.get(isbn)
        e = map_erp.get(isbn)
        
        # Só compara se o item existir nos dois lados (cruzamento perfeito)
        if x and e:
            # Compara Quantidades
            qtd_x = float(x.get('Quantidade', 0))
            qtd_e = float(e.get('Quant', 0))
            if qtd_x != qtd_e: c_qtd += 1
            
            # Compara Preços Unitários (Calculando a partir do total se necessário)
            val_liq_total_x = float(x.get('Valor_Liquido', 0))
            val_unit_x = val_liq_total_x / qtd_x if qtd_x > 0 else 0
            val_unit_e = float(e.get('VlLiqUnit', 0))
            
            # Considera divergência apenas se a diferença for maior que 1 centavo
            if abs(val_unit_x - val_unit_e) > 0.01: c_preco += 1

    msgs = []
    if c_qtd > 0: msgs.append(f"Qtde Diferente ({c_qtd}x)")
    if c_preco > 0: msgs.append(f"Preço Diferente ({c_preco}x)")
    
    if not msgs:
        if not itens_xml: return "XML Vazio"
        return "" # Se vazio, está tudo OK
    return " | ".join(msgs)

# --- TAREFA PRINCIPAL (THREAD) ---

def tarefa_background(modulo, app_config):
    """
    Função principal que é executada em segundo plano.
    Recebe 'app_config' manualmente porque dentro de uma Thread nova, 
    o Flask perde o acesso às configurações globais (current_app).
    """
    caminho_xml = app_config.get('CAMINHO_XML_PADRAO')
    
    # Define as regras com base no módulo escolhido pelo usuário
    if modulo == 'devolucao':
        # Devolução filtra por CFOPs específicos (saída) e Tipo Pedido 4
        cfops = ['5917', '6917']
        tipo_pedido = 4
        arquivo_cache = CACHE_DEVOLUCAO

    elif modulo == 'acerto':
        # --- CORREÇÃO 1: FILTRO DE CFOP ---
        # Agora usamos a lista configurada no config.py em vez de None
        cfops = app_config.get('CFOPS_PADRAO')
        tipo_pedido = 1
        arquivo_cache = CACHE_ACERTO
        
    else: 
        # Leitor Geral
        cfops = None
        tipo_pedido = 1
        arquivo_cache = CACHE_GERAL

    STATUS_GLOBAL['msg'] = 'Lendo arquivos XML...'
    
    # IMPORTANTE: Reimportamos a função aqui dentro para usar uma versão que
    # não dependa do contexto web do Flask, evitando erros de "Application Context".
    from app.services.xml_service import processar_pasta_xml_thread_safe 
    
    # 1. Varredura dos arquivos físicos
    df_xml, msg = processar_pasta_xml_thread_safe(caminho_xml, cfops, atualizar_progresso, app_config['PASTAS_IGNORADAS'])
    
    if df_xml.empty:
        STATUS_GLOBAL['status'] = 'concluido'
        STATUS_GLOBAL['msg'] = msg or "Nenhum XML encontrado"
        return

    STATUS_GLOBAL['msg'] = 'Cruzando dados com ERP...'
    
    # 2. Cruzamento com Lojas (Filiais)
    # Tenta descobrir o nome da loja pelo CNPJ do destinatário da nota
    if 'CNPJ_Destinatario' in df_xml.columns:
        df_xml['KEY_CNPJ'] = df_xml['CNPJ_Destinatario'].apply(limpar_cnpj)
        df_lojas_erp = buscar_filiais() # Busca no SQL
        
        if not df_lojas_erp.empty:
            df_lojas_erp['KEY_CNPJ'] = df_lojas_erp['CNPJ'].apply(limpar_cnpj)
            # 'pd.merge' é como um PROCV do Excel
            df_merged = pd.merge(df_xml, df_lojas_erp[['KEY_CNPJ', 'Nome_Filial']], on='KEY_CNPJ', how='left')
            
            # Se achou no banco, usa. Se não, usa o nome que estava no XML.
            df_merged['Filial'] = df_merged['Nome_Filial'].fillna(df_merged['Nome_Destinatario'])
            df_merged['Filial'] = df_merged['Filial'].fillna("Filial Não Identificada")
            df_xml = df_merged.drop(columns=['Nome_Filial'])
        else:
            df_xml['Filial'] = df_xml.get('Nome_Destinatario', 'Sem Nome')
    else:
        df_xml['Filial'] = 'Sem Destinatário'

    # 3. Cruzamento com Fornecedores
    # Busca dados adicionais (prazo, dia acerto) dos fornecedores de consignação
    df_forn = buscar_dados_fornecedores()
    if not df_forn.empty:
        df_xml['KEY_EMIT'] = df_xml['CNPJ_Emitente'].apply(limpar_cnpj)
        df_forn['KEY_EMIT'] = df_forn['CNPJ'].apply(limpar_cnpj)
        df_final = pd.merge(df_xml, df_forn, on='KEY_EMIT', how='left')
    else: df_final = df_xml.copy()

    # Preenche campos vazios para ficar bonito na tabela
    cols = ['Nome_Fantasia', 'Filial', 'Prazo', 'Dia_Acerto']
    for c in cols:
        if c not in df_final.columns: df_final[c] = '-'
        else: df_final[c] = df_final[c].fillna('-')
        
    df_final = df_final.fillna("")
    
    # Converte de Tabela (DataFrame) para Lista de Dicionários (JSON)
    lista = df_final.to_dict('records')
    
    # 4. Busca de Pedidos Vinculados no ERP
    # Pega todos os números de pedido que estavam no campo 'xPed' dos XMLs
    pedidos = [n.get('Numero_Pedido') for n in lista if n.get('Numero_Pedido')]
    
    df_itens_erp = pd.DataFrame()
    if pedidos:
        STATUS_GLOBAL['msg'] = f'Buscando {len(pedidos)} pedidos no Banco...'
        # Busca itens de TODOS os pedidos de uma vez só (muito mais rápido que buscar um por um)
        df_itens_erp = buscar_itens_pedidos_lote(pedidos, tipo_acerto_alvo=tipo_pedido)
        if not df_itens_erp.empty:
            # Converte datas para texto para não quebrar o JSON
            for col in df_itens_erp.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df_itens_erp[col] = df_itens_erp[col].astype(str)

    STATUS_GLOBAL['msg'] = 'Finalizando análises...'
    
    # 5. Cruzamento Final Item a Item
    for nota in lista:
        
        # --- CORREÇÃO 2: TÍTULO E PREÇO (SIMPLIFICADA) ---
        # Garante que os campos que o site espera (Titulo, Valor_Liquido) existam
        for item in nota.get('Itens', []):
            # Se não tiver Titulo, usa xProd (do XML)
            item.setdefault('Titulo', item.get('xProd', 'Produto Sem Nome'))
            
            # Se não tiver Valor_Liquido, usa vProd (do XML) e converte para número
            if 'Valor_Liquido' not in item:
                try: item['Valor_Liquido'] = float(item.get('vProd', 0))
                except: item['Valor_Liquido'] = 0.0
            
            # Garante quantidade como número
            if 'Quantidade' in item:
                try: item['Quantidade'] = float(item['Quantidade'])
                except: item['Quantidade'] = 0.0
        # --- FIM DA CORREÇÃO ---

        # Aqui estava o erro: A variável 'ped' precisa ser criada aqui!
        ped = str(nota.get('Numero_Pedido', ''))
        nota['Itens_ERP'] = []
        
        # Se essa nota tem pedido e trouxemos dados do banco, anexa os itens
        if ped and not df_itens_erp.empty:
            itens = df_itens_erp[df_itens_erp['Numero_Pedido_Chave'] == ped]
            if not itens.empty: nota['Itens_ERP'] = itens.to_dict('records')
        
        # Roda a função de comparação para gerar o resumo (ex: "Preço Diferente")
        nota['Divergencia_Resumo'] = gerar_resumo_divergencia(nota)
        
        if 'Valor_Total' in nota: nota['Valor_Total'] = formatar_moeda(nota['Valor_Total'])

    # 6. Salva no Disco (Cache)
    ts = datetime.now().strftime("%d/%m/%Y às %H:%M")
    try:
        with open(arquivo_cache, 'w', encoding='utf-8') as f:
            json.dump({"timestamp": ts, "dados": lista}, f, ensure_ascii=False, indent=4)
    except: pass

    # Avisa que acabou
    STATUS_GLOBAL['status'] = 'concluido'
    STATUS_GLOBAL['msg'] = 'Concluído!'