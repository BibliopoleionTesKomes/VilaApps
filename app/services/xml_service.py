import os
import pandas as pd
import xml.etree.ElementTree as ET # Biblioteca padrão para ler arquivos XML
from datetime import datetime
import concurrent.futures # Biblioteca para processamento paralelo (vários arquivos ao mesmo tempo)
from flask import current_app

# --- FUNÇÃO DE PARSE (EXTRAÇÃO DE DADOS) ---

def parse_nfe(xml_path, cfops_filtro=None, cfops_config=None):
    """
    Lê um arquivo XML individual e extrai os dados da Nota Fiscal.
    
    Args:
        xml_path (str): Caminho do arquivo no computador.
        cfops_filtro (list): Lista de CFOPs específicos para filtrar (ex: Devolução).
        cfops_config (list): Lista padrão de CFOPs aceitos (passado manualmente para evitar erro de thread).
    """
    
    # Lógica de segurança para Threads:
    # Se estamos rodando no site normal, pegamos do current_app.
    # Se estamos rodando no robô (thread), usamos o parametro recebido.
    if cfops_config:
        cfops_padrao = cfops_config
    else:
        # Tenta pegar do app, se falhar (estiver em thread sem config), usa lista vazia
        try: cfops_padrao = current_app.config['CFOPS_PADRAO']
        except: cfops_padrao = []
            
    # Define quais CFOPs vamos aceitar
    cfops_validos = cfops_filtro if cfops_filtro is not None else cfops_padrao
    
    try:
        # Abre e lê o arquivo XML
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Define o "namespace" (o link que aparece no topo de todo XML de nota)
        # Sem isso, o Python não encontra as tags.
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        
        # Verifica se é uma NFe válida procurando a tag principal
        inf_nfe = root.find('.//nfe:infNFe', ns)
        if inf_nfe is None: return None

        # --- EXTRAÇÃO DE DADOS (CABEÇALHO) ---
        
        # Função auxiliar para buscar texto de uma tag com segurança (se não existir, retorna vazio)
        def get_tag(path):
            el = root.find(path, ns)
            return el.text if el is not None else ""

        # Dados Básicos
        ide = root.find('.//nfe:ide', ns)
        emit = root.find('.//nfe:emit', ns)
        dest = root.find('.//nfe:dest', ns)
        
        if not ide or not emit: return None # Nota inválida sem emissor

        # Extração de campos chave
        dados = {
            'Chave_Acesso': (inf_nfe.attrib.get('Id', '')[3:]), # Remove o prefixo 'NFe' da chave
            'Numero_NF': get_tag('.//nfe:ide/nfe:nNF'),
            'Serie': get_tag('.//nfe:ide/nfe:serie'),
            'Data_Emissao': get_tag('.//nfe:ide/nfe:dhEmi')[:10], # Pega só YYYY-MM-DD
            'CNPJ_Emitente': get_tag('.//nfe:emit/nfe:CNPJ'),
            'Nome_Emitente': get_tag('.//nfe:emit/nfe:xNome'),
            'Nome_Fantasia': get_tag('.//nfe:emit/nfe:xFant'),
            'CNPJ_Destinatario': get_tag('.//nfe:dest/nfe:CNPJ'),
            'Nome_Destinatario': get_tag('.//nfe:dest/nfe:xNome'),
            'Valor_Total': float(get_tag('.//nfe:total/nfe:ICMSTot/nfe:vNF') or 0),
            'Itens': [],
            'CFOPs': set(), # Usamos 'set' para guardar CFOPs únicos sem repetir
            'Numero_Pedido': '' # Será preenchido se acharmos a tag xPed
        }
        
        # Formatação da Data (Americano -> Brasileiro)
        if dados['Data_Emissao']:
            try:
                dados['Data_Emissao'] = datetime.strptime(dados['Data_Emissao'], '%Y-%m-%d').strftime('%d/%m/%Y')
            except: pass

        # --- EXTRAÇÃO DE ITENS (PRODUTOS) ---
        # Varre todos os produtos da nota (tag 'det')
        for det in root.findall('.//nfe:det', ns):
            prod = det.find('nfe:prod', ns)
            if prod is None: continue
            
            cfop = prod.find('nfe:CFOP', ns).text
            
            # FILTRAGEM: Se o CFOP não estiver na nossa lista de permitidos, ignora este item
            if cfops_validos and cfop not in cfops_validos:
                continue
                
            dados['CFOPs'].add(cfop)
            
            # Tenta pegar o número do pedido no campo xPed do item
            if not dados['Numero_Pedido']:
                dados['Numero_Pedido'] = prod.find('nfe:xPed', ns).text if prod.find('nfe:xPed', ns) is not None else ""

            item = {
                'ISBN': prod.find('nfe:cEAN', ns).text,
                'Descricao': prod.find('nfe:xProd', ns).text,
                'Quantidade': float(prod.find('nfe:qCom', ns).text or 0),
                'Valor_Unitario': float(prod.find('nfe:vUnCom', ns).text or 0),
                'Valor_Total': float(prod.find('nfe:vProd', ns).text or 0),
                'CFOP': cfop
            }
            dados['Itens'].append(item)

        # SE A NOTA NÃO TIVER NENHUM ITEM COM CFOP VÁLIDO, ELA É DESCARTADA
        if not dados['Itens']:
            return None
            
        # Converte o conjunto de CFOPs para string (ex: "5102, 5405")
        dados['CFOPs'] = ", ".join(dados['CFOPs'])
        
        # --- CÁLCULO DE VENCIMENTO ---
        dup = root.find('.//nfe:cobr/nfe:dup', ns)
        if dup:
            venc = dup.find('nfe:dVenc', ns)
            if venc and venc.text:
                try:
                    dt_obj = datetime.strptime(venc.text, '%Y-%m-%d')
                    dados['Data_Vencimento'] = dt_obj.strftime('%d/%m/%Y')
                    dados['Dia_Acerto'] = dt_obj.day
                    
                    # Cálculo simples de prazo (dias entre emissão e vencimento)
                    if dados['Data_Emissao']:
                        dt_emi = datetime.strptime(dados['Data_Emissao'], '%d/%m/%Y')
                        delta = dt_obj - dt_emi
                        dados['Prazo'] = f"{delta.days} dias"
                except:
                    dados['Data_Vencimento'] = venc.text
        else:
            dados['Data_Vencimento'] = '-'
            dados['Prazo'] = 'A Vista'
            
        return dados
        
    except Exception as e:
        # Se o XML estiver corrompido, retorna None para não travar o processo
        return None

# --- FUNÇÃO THREAD-SAFE (PARA O ROBÔ) ---

def processar_pasta_xml_thread_safe(caminho_pasta, cfops_filtro, callback_progresso, pastas_ignoradas):
    """
    Versão segura para Threads da função de processamento.
    Recebe 'pastas_ignoradas' como argumento em vez de ler do current_app.
    """
    # Garante que o caminho do Windows esteja correto
    caminho = os.path.normpath(caminho_pasta)
    
    if not os.path.exists(caminho): 
        return pd.DataFrame(), "Pasta não encontrada"

    arquivos_para_ler = []
    
    # 1. VARREDURA (LISTAGEM) DOS ARQUIVOS
    # os.walk percorre todas as subpastas
    for root, dirs, files in os.walk(caminho):
        # Remove pastas ignoradas da busca
        dirs[:] = [d for d in dirs if d.lower().strip() not in pastas_ignoradas]
        
        # Se a pasta atual for ignorada, pula
        if any(p in os.path.normpath(root).lower().split(os.sep) for p in pastas_ignoradas): 
            continue
            
        for f in files:
            if f.lower().endswith('.xml'): 
                arquivos_para_ler.append(os.path.join(root, f))
    
    total = len(arquivos_para_ler)
    if total == 0: return pd.DataFrame(), "Nenhum XML encontrado."

    # Avisa o painel de controle que começou (0%)
    if callback_progresso: callback_progresso(0, total)

    lista_dados = []
    lidos = 0
    
    # 2. PROCESSAMENTO PARALELO (MULTITHREADING)
    # Aqui a mágica acontece. O Python abre 20 "operários" para ler 20 arquivos ao mesmo tempo.
    # Isso torna a leitura de milhares de notas incrivelmente rápida.
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        
        # Cria as tarefas
        # Passamos 'cfops_filtro' (se é acerto ou devolução)
        # Passamos 'cfops_padrao' vazio ou preenchido (não usamos current_app aqui)
        futures = {executor.submit(parse_nfe, xml, cfops_filtro, cfops_filtro): xml for xml in arquivos_para_ler}
        
        # Conforme cada arquivo termina de ser lido...
        for future in concurrent.futures.as_completed(futures):
            lidos += 1
            # Atualiza a barra de progresso
            if callback_progresso: callback_progresso(lidos, total)
            
            try:
                res = future.result()
                if res: lista_dados.append(res) # Se a nota for válida, adiciona na lista
            except: pass

    # 3. CONSOLIDAÇÃO
    df = pd.DataFrame(lista_dados)
    
    # Normalização para garantir que o DataFrame sempre tenha as colunas esperadas
    cols_obrig = ['CNPJ_Destinatario', 'CNPJ_Emitente', 'Numero_Pedido', 'Valor_Total', 'Data_Vencimento']
    if not df.empty:
        for col in cols_obrig:
            if col not in df.columns: 
                df[col] = '' if col != 'Valor_Total' else 0.0
    else:
        # Se não leu nada, retorna dataframe vazio com colunas
        df = pd.DataFrame(columns=cols_obrig)

    return df, None

# --- FUNÇÃO PADRÃO (PARA USO DIRETO NO SITE) ---

def processar_pasta_xml(caminho_pasta, cfops_filtro=None, callback_progresso=None):
    """
    Função Wrapper (Embrulho). 
    Ela pega as configs do 'current_app' e chama a versão segura.
    Usada quando clicamos no botão "Atualizar" manualmente na tela, fora da thread.
    """
    pastas_ignoradas = current_app.config['PASTAS_IGNORADAS']
    return processar_pasta_xml_thread_safe(caminho_pasta, cfops_filtro, callback_progresso, pastas_ignoradas)