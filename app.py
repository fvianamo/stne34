import pandas as pd
import numpy as np
from datetime import datetime

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



if __name__=='__main__':
    df = pd.read_csv('eventos.csv', dtype={'id': str, 'preço': np.float64, 'evento': str, 'data_inicial': str, 'data_final': str})
    
    #parsing dates
    df['data_inicial'] = pd.to_datetime(df['data_inicial'], format='%d-%m-%Y')
    df['data_final'] = pd.to_datetime(df['data_final'], format='%d-%m-%Y')
    
    df['check_dtf'] = df.apply(check_data_final, axis=1)
    df['check_dtfl'] = df.apply(check_data_final_limit, axis=1)
    df['check_dtu'] = check_data_inicial(df)
    df['check_pp'] = df.apply(check_periodo_promocional, axis=1)
    print(df.head(100))