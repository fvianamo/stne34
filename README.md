# Processador de faturas

Esse script tem como objetivo o processamento de uma lista de eventos das maquininhas. Os dados de entrada são disponibilizados no formato csv e apresentam as seguintes informações:
* id: Identificado da maquininha  (string)
* preço: Preço da maquininha definido para aquele ponto (decimal)
* evento: Tipo de evento da maquininha (string) [Ativação, Desativação, Mudança de Preço, Período Promocional]
* data_inicial: Validade inicial do preço (date)
* data_final: Validade final do preço (date)

Os dados importados são inicialmente validados quanto ao atendimento de uma série de requisitos.

Em seguida, os dados são processados para gerar um visão agregada do valor a ser faturado por maquininha.