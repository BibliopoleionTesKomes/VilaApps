import os
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import concurrent.futures

CFOPS_PADRAO = ['5113', '5114', '6113', '6114', '1113', '1114', '2113', '2114']
PASTAS_IGNORADAS = ['enviados', 'associado', 'associados', 'canceladas', 'inutilizadas'] 

def parse_nfe(xml_path, cfops_filtro=None):
    """Função worker: Processa um único arquivo XML."""
    cfops_validos = cfops_filtro if cfops_filtro is not None else CFOPS_PADRAO
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        # Namespace padrão da NFe
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        
        # Localiza infNFe
        inf_nfe = root.find('.//nfe:infNFe', ns)
        if inf_nfe is None: return None

        cfops_encontrados = set()
        lista_itens = []
        pedido_encontrado = None
        
        # Busca Pedido Global
        compra = inf_nfe.find('nfe:compra', ns)
        if compra is not None:
            xped = compra.find('nfe:xPed', ns)
            if xped is not None: pedido_encontrado = str(xped.text).strip()

        # Processa Itens
        det_list = inf_nfe.findall('nfe:det', ns)
        for det in det_list:
            prod = det.find('nfe:prod', ns)
            if prod:
                cfop_el = prod.find('nfe:CFOP', ns)
                if cfop_el is not None: cfops_encontrados.add(cfop_el.text)
                
                if not pedido_encontrado:
                    xped = prod.find('nfe:xPed', ns)
                    if xped is not None: pedido_encontrado = str(xped.text).strip()

                vprod = float(prod.find('nfe:vProd', ns).text or 0)
                vdesc = float(prod.find('nfe:vDesc', ns).text or 0) if prod.find('nfe:vDesc', ns) is not None else 0.0
                qcom = float(prod.find('nfe:qCom', ns).text or 0)
                xprod = prod.find('nfe:xProd', ns).text
                # EAN/ISBN
                cean_el = prod.find('nfe:cEAN', ns)
                cean = cean_el.text if cean_el is not None else ""

                lista_itens.append({
                    'ISBN': cean, 'Titulo': xprod, 'Quantidade': qcom,
                    'Valor_Bruto': vprod, 'Valor_Liquido': vprod - vdesc
                })
        
        # Filtro de CFOP
        if cfops_validos and not cfops_encontrados.intersection(set(cfops_validos)):
            return None 

        # Dados Gerais
        ide = inf_nfe.find('nfe:ide', ns)
        emit = inf_nfe.find('nfe:emit', ns)
        dest = inf_nfe.find('nfe:dest', ns)
        total = inf_nfe.find('nfe:total/nfe:ICMSTot', ns)
        
        def get_val(node, tag):
            if node is None: return ""
            el = node.find(f'nfe:{tag}', ns)
            return el.text if el is not None else ""

        # Data Emissão
        raw_date = get_val(ide, 'dhEmi') or get_val(ide, 'dEmi')
        data_fmt = ""
        if raw_date:
            try: data_fmt = datetime.strptime(raw_date[:10], '%Y-%m-%d').strftime('%d/%m/%Y')
            except: pass

        # --- CORREÇÃO VENCIMENTO ---
        # Busca a tag dVenc em QUALQUER LUGAR dentro de infNFe.
        # Isso resolve casos onde a hierarquia cobr/dup varia ou o namespace falha.
        vencimento_fmt = ""
        el_venc = inf_nfe.find('.//nfe:dVenc', ns)
        if el_venc is not None and el_venc.text:
            try: 
                vencimento_fmt = datetime.strptime(el_venc.text[:10], '%Y-%m-%d').strftime('%d/%m/%Y')
            except: pass

        chave = inf_nfe.attrib.get('Id', '')[3:]

        dados = {
            'Arquivo': os.path.basename(xml_path),
            'CFOPs': ", ".join(sorted(cfops_encontrados)),
            'Numero_Pedido': pedido_encontrado or "",
            'Numero_NF': get_val(ide, 'nNF'),
            'Serie': get_val(ide, 'serie'),
            'Data_Emissao': data_fmt,
            'Data_Vencimento': vencimento_fmt, # Campo Corrigido
            'CNPJ_Emitente': get_val(emit, 'CNPJ'),
            'Nome_Emitente': get_val(emit, 'xNome'),
            'CNPJ_Destinatario': get_val(dest, 'CNPJ'),
            'Nome_Destinatario': get_val(dest, 'xNome'),
            'Valor_Total': float(total.find('nfe:vNF', ns).text) if total is not None and total.find('nfe:vNF', ns) is not None else 0.0,
            'Chave_Acesso': chave,
            'Itens': lista_itens
        }
        return dados
    except: return None

def processar_pasta_xml(caminho_pasta, cfops_filtro=None, callback_progresso=None):
    """
    Lê a pasta usando ThreadPoolExecutor.
    Aceita um callback para atualizar barra de progresso.
    """
    caminho = os.path.normpath(caminho_pasta)
    if not os.path.exists(caminho): return pd.DataFrame(), "Pasta não encontrada"

    arquivos_para_ler = []
    for root, dirs, files in os.walk(caminho):
        dirs[:] = [d for d in dirs if d.lower().strip() not in PASTAS_IGNORADAS]
        if any(p in os.path.normpath(root).lower().split(os.sep) for p in PASTAS_IGNORADAS): continue
        for f in files:
            if f.lower().endswith('.xml'): arquivos_para_ler.append(os.path.join(root, f))
    
    total_arquivos = len(arquivos_para_ler)
    if total_arquivos == 0: return pd.DataFrame(), "Nenhum XML encontrado."

    if callback_progresso: callback_progresso(0, total_arquivos)

    lista_dados = []
    lidos_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(parse_nfe, xml, cfops_filtro): xml for xml in arquivos_para_ler}
        for future in concurrent.futures.as_completed(futures):
            lidos_count += 1
            if callback_progresso: callback_progresso(lidos_count, total_arquivos)
            try:
                resultado = future.result()
                if resultado: lista_dados.append(resultado)
            except: pass

    df = pd.DataFrame(lista_dados)
    # Garante colunas mínimas para não quebrar o app
    cols_obrig = ['CNPJ_Destinatario', 'CNPJ_Emitente', 'Numero_Pedido', 'Valor_Total', 'Data_Vencimento']
    for col in cols_obrig:
        if col not in df.columns: df[col] = '' if col != 'Valor_Total' else 0.0

    return df, None