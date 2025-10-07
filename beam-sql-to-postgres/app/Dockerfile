# Usa uma imagem Python oficial
FROM python:3.9-slim

# Define o diretório de trabalho
WORKDIR /usr/src/app

# Instala dependências do sistema necessárias para o psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia o arquivo de dependências
COPY requirements.txt ./

# Instala as dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código da aplicação
COPY . .

# Comando para rodar a aplicação
CMD ["python", "main.py"]