[Extração MySQL] --> [Transformação com Python] --> [Armazenamento GCP]
    |                                               |
    |                                               |
    v                                               v
[Banco de Dados MySQL] --> [Limpeza e Transformação] --> [Bucket da GCP]
    |                                               |
    |                                               |
    v                                               v
[Arquivos CSV] --> [Processamento dos Dados] --> [Armazenamento Destino (GCP)]
                                                        |
                                                        |
                                                        v
                                           [Looker Studio Dashboard]
