# Processador de faturas

Esse script tem como objetivo o processamento de uma lista de eventos das maquininhas. Os dados de entrada são disponibilizados no formato csv e apresentam as seguintes informações:
* id: Identificado da maquininha  (string)
* preço: Preço da maquininha definido para aquele ponto (decimal)
* evento: Tipo de evento da maquininha (string) [Ativação, Desativação, Mudança de Preço, Período Promocional]
* data_inicial: Validade inicial do preço (date)
* data_final: Validade final do preço (date)

Os dados de entrada são submetidos ao processo ilustados na imagem abaixo.

![processo](stne34.png)

A primeira etapa do processo é a realização de uma checagem de qualidade do registro, onde cada requisito é implementado como um método, e caso o registro passe por todos os requisitos ele será considerado nas próximas etapas, caso contrário ele será desconsiderado.

Com o conjunto de dados integros, realiza-se então a quebra do dataset, resultando em uma lista de dataset sendo um para cada maquininha sendo faturada.

Cada dataset de maquininha é então submetido a um pre processamento, que irá formatar os períodos de faturamento, indicando seu início, fim e preço, a partir dos eventos capturados pelo equipamento. 

Com os dados pre processando, então, calcula-se o valor da fatura para o mês, conforme regra estabelecida.

Por fim, os dados de fatura de cada maquininha são compostos em um único dataset para exportação do relatório.

## como executar?

Para executar o script, recomenda-se a inicialização e ativação de um ambiente virtual por meio do seguinte comando

```bash
virtualenv -p python 3 .venv &&
source .venv/bin/activate
```

Uma vez dentro do ambiente virtual instale as dependências do script utilizando o seguinte comando

```bash
pip install -r requirements.txt
```

Por fim, realize a chamada do script informando o path do arquivo de entrada, comforme exemplo abaixo

```bash
python app.py eventos.csv 
```

Considerando que o processo rodou corretamente, os resultados estarão disponíveis no arquivo fatura.csv neste mesmo diretório.