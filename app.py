import pandas as pd
import numpy as np
from datetime import datetime
import logging
import argparse

logging.basicConfig( format='%(asctime)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s', level=logging.INFO)

def parse_arguments():
    """
    Método auxiliar para captura do path do arquivo de entrada nos
    argumentos informados na chamada do script
    """
    parser = argparse.ArgumentParser(
        description="Utilitário para geração de fatura de maquininhas.")

    parser.add_argument("source",
                        metavar="source",
                        type=str,
                        help="caminho do arquivo de entrada")

    return parser.parse_args()


def check_data_final(event):
    """
    Método retorna True caso o requisito de data final maior que data inicial 
    seja atendido para o evento e False caso o requisito não seja atendido 
    """
    if not pd.isnull(event['data_final']):
        return True if event['data_final'] > event['data_inicial'] else False
    else:
        return True


def check_data_final_limit(event, month=1):
    """
    Método retorna True caso o requisito de data final dentro do mês de processamento 
    seja atendido para o evento e False caso o requisito não seja atendido 
    """
    if not pd.isnull(event['data_final']):
        return True if event['data_final'].month == month else False
    else:
        return True


def check_data_inicial(df_event):
    """
    Método retorna pandas.Series com resultado da checagem do requisito de unicidade 
    de data_inicial nos registros de maquininha
    """
    df_event = df_event.sort_values(['id', 'data_inicial'], ascending=True)
    df_event['data_inicial_y'] = df_event.groupby(['id'])['data_inicial'].shift(-1)
    return df_event.apply(lambda row: (row['data_inicial'] < row['data_inicial_y']) or pd.isnull(row['data_inicial_y']), axis=1)


def check_periodo_promocional(event):
    """
    Método retorna True caso o requisito de apenas eventos de Período Promocional
    devem ter data_final seja atendido para o evento e False caso o requisito não 
    seja atendido 
    """
    if not pd.isnull(event['data_final']):
        return event['evento'] == 'Período Promocional'
    else:
        return True


def check_nan_desativacao(event):
    """
    Método retorna True caso o requisito de apenas eventos de Desativação podem 
    ter preço não informado seja atendido para o evento e False caso o requisito não 
    seja atendido 
    """
    if pd.isnull(event['preço']):
        return event['evento'] == 'Desativação'
    else:
        return True


def check_zero_promocional(event):
    """
    Método retorna True caso o requisito de apenas eventos de Período Promocional podem
    ter preço zerado seja atendido para o evento e False caso o requisito não 
    seja atendido 
    """
    if event['preço'] == 0:
        return event['evento'] == 'Período Promocional'
    else:
        return True


def check_promocional_overlap(df_event):
    """
    Método retorna pandas.Series com resultado da checagem de intercessão de
    períodos promocionais
    """
    df_event = df_event.sort_values(['id', 'evento', 'data_inicial'], ascending=True)
    df_event['data_final_y'] = df_event.groupby(['id', 'evento'])['data_final'].shift(1)
    return df_event.apply(lambda row: not((row['data_inicial'] < row['data_final_y']) and row['evento'] == 'Período Promocional'), axis=1)


def prep_billing(df, ultimo_dia=datetime(2021, 1, 31)):
    """
    Método retorna DataFrame resultante do processo de prepração dos eventos
    da maquineta. Nele, os eventos são convertidos em períodos de faturamento,
    com suas devidas datas de inicio e fim, preço praticado no período e tipo
    do período.
    """
    list_events = df.sort_values('data_inicial').to_dict('records')

    report = []
    state = 'nao ativo'
    for event in list_events:
        if state == 'nao ativo' and event.get('evento') == 'Ativação':
            logging.debug('ativando maquininha')
            state = 'ativo'
            report.append(dict(tipo='regular', 
                            data_inicial=event.get("data_inicial"),
                            preco=event.get("preço")))
            
        elif state == 'ativo' and event.get('evento') == 'Período Promocional':
            logging.debug('Incluindo periodo promocional em maquininha ativa')
            state = 'ativo'
            report[-1]['data_final'] = event.get("data_inicial")
            report.append(dict(tipo='promocional', 
                            data_inicial=event.get("data_inicial"),
                            preco=event.get("preço"),
                                data_final=event.get("data_final")))
            report.append(dict(tipo='pos promocional',
                            data_inicial=event.get("data_final"),
                            preco=report[-2].get('preco')))
            
        elif state == 'ativo' and event.get('evento') in ['Ativação', 'Mudança de Preço']:
            if report[-1].get('tipo') == 'pos promocional':
                logging.debug('Atualizando preço para periodo pós promocional')
                report[-1]['preco'] = event.get('preço')
            else:
                logging.debug('Atualizando preço')
                report[-1]['data_final'] = event.get('data_inicial')
                report.append(dict(tipo='regular', 
                            data_inicial=event.get("data_inicial"),
                            preco=event.get("preço")))
        elif state == 'ativo' and event.get('evento') == 'Desativação':
            state = 'nao ativo'
            if report[-1].get('tipo') == 'pos promocional':
                logging.debug('encerrando periodo pos promocional por desativação')
                report[-1]['data_final'] = report[-1]['data_inicial']
            else:
                logging.debug('finalizando periodo de faturamento da maquininha')
                report[-1]['data_final'] = event.get('data_inicial')

    if not report == []:
        if report[-1].get('data_final'):
            logging.debug('maquininha finalizou o mês não ativa')
        else:
            logging.debug('maquininha finalizou o mês ativa')
            report[-1]['data_final'] = ultimo_dia

    return pd.DataFrame(report)


def calc_billing(df):
    """
    Método retorna o valor de fatura a partir dos dados presentes 
    no DataFrame de input, que deve ser resultado do método
    prep_billing
    """
    rows, cols = df.shape
    if rows == 0:
        logging.info('sem eventos para a maquininha')
        return 0

    logging.info(f'calculando fatura da maquininha com {rows} eventos')
    df['price_event'] = df.apply(lambda row: ((row['data_final'] - row['data_inicial']).days * row['preco']), axis=1)
    return df['price_event'].sum()/30
        

if __name__=='__main__':
    """
    Main do script que "orquestra" o processamento do arquivo
    resultando no arquivo de fatura salvo neste diretório com
    nome "fatura.csv"
    """
    args = parse_arguments()

    logging.info('Carregando dataset de eventos...')
    df = pd.read_csv(args.source, dtype={'id': str, 'preço': np.float64, 'evento': str, 'data_inicial': str, 'data_final': str})
    logging.info(f'Dataset carregado. shape: {df.shape}')
    #Arredondando preço
    df = df.round({'preço': 2})

    #Realizando parsing das datas
    df['data_inicial'] = pd.to_datetime(df['data_inicial'], format='%d-%m-%Y')
    df['data_final'] = pd.to_datetime(df['data_final'], format='%d-%m-%Y')
    
    logging.info('Iniciando check de qualidade dos eventos')
    #Check de qualidade
    df['check_dtf'] = df.apply(check_data_final, axis=1)
    df['check_dtfl'] = df.apply(check_data_final_limit, axis=1)
    df['check_dtu'] = check_data_inicial(df)
    df['check_pp'] = df.apply(check_periodo_promocional, axis=1)
    df['check_zpp'] = df.apply(check_zero_promocional, axis=1)
    df['check_desat'] = df.apply(check_nan_desativacao, axis=1)
    df['check_pp_over'] = check_promocional_overlap(df)
    df['check'] = df[['check_dtf', 'check_dtfl', 'check_dtu', 'check_pp', 'check_zpp', 'check_desat', 'check_pp_over']].all(axis=1)
    
    #droping 
    df_clean = df[df['check']==True][['id', 'preço', 'evento', 'data_inicial', 'data_final']]
    logging.info(f'Dataset com registo validos ficou com o seguinte shape {df_clean.shape}')

    logging.info('Quebrando um dataset para cada maquininha')
    device_dfs = [df_id for i, df_id in df_clean.groupby('id')]
    fatura = []
    for df in device_dfs:
        df = df.reset_index()
        logging.info(f'calculando fatura para maquininha {df.at[0, "id"]}')
        df_preped = prep_billing(df)
        fatura.append(dict(id=df.at[0, "id"],
                           preco=calc_billing(df_preped)))
    pd.DataFrame(fatura).round({'preco': 2}).to_csv('fatura.csv', index=False)