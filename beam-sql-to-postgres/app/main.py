import os
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# --- DoFn para LER do banco de dados de origem ---
class ReadFromSourceDB(beam.DoFn):
    def __init__(self, conn_string, query):
        self._conn_string = conn_string
        self._query = query

    def process(self, element):
        # Esta DoFn será iniciada com um único elemento e então executará a query
        conn = None
        try:
            logging.info("Conectando ao banco de dados de ORIGEM...")
            conn = psycopg2.connect(self._conn_string)
            cursor = conn.cursor()
            cursor.execute(self._query)
            
            # Pega os nomes das colunas
            col_names = [desc[0] for desc in cursor.description]
            
            # Emite cada linha como um dicionário
            for row in cursor.fetchall():
                yield dict(zip(col_names, row))

        except Exception as e:
            logging.error(f"Erro ao ler do banco de origem: {e}")
        finally:
            if conn:
                conn.close()
                logging.info("Conexão de ORIGEM fechada.")

# --- DoFn para ESCREVER no banco de dados de destino ---
class WriteToAlloyDB(beam.DoFn):
    def __init__(self, conn_string, table_name):
        self._conn_string = conn_string
        self._table_name = table_name
        self.conn = None
        self.cursor = None

    def setup(self):
        logging.info("Abrindo conexão com o AlloyDB (Simulador)...")
        try:
            self.conn = psycopg2.connect(self._conn_string)
            self.cursor = self.conn.cursor()
            
            # Garante que o schema e a tabela existam no destino
            schema, table = self._table_name.split('.')
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            # Usa a mesma estrutura da tabela de origem
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_name} (
                    id INTEGER PRIMARY KEY,
                    nome VARCHAR(100),
                    categoria VARCHAR(50),
                    valor NUMERIC(10, 2),
                    data_criacao TIMESTAMP WITH TIME ZONE
                );
            """)
            self.conn.commit()
            logging.info(f"Tabela '{self._table_name}' garantida no destino.")
        except Exception as e:
            logging.error(f"Não foi possível conectar ou preparar o AlloyDB (Simulador): {e}")
            raise e

    def process(self, element):
        try:
            columns = element.keys()
            values = [element[col] for col in columns]
            
            # Usando ON CONFLICT para evitar erros de chave primária duplicada
            insert_sql = f"""
                INSERT INTO {self._table_name} ({', '.join(columns)}) 
                VALUES ({', '.join(['%s'] * len(values))})
                ON CONFLICT (id) DO UPDATE SET
                    nome = EXCLUDED.nome,
                    categoria = EXCLUDED.categoria,
                    valor = EXCLUDED.valor;
            """
            
            self.cursor.execute(insert_sql, values)
            logging.info(f"Registro {element.get('id')} salvo no destino.")
            yield (element.get('id'), 'SALVO_COM_SUCESSO')

        except Exception as e:
            logging.error(f"Falha ao inserir o elemento {element.get('id')}: {e}")
            self.conn.rollback()
            yield (element.get('id'), f'ERRO: {e}')
    
    def teardown(self):
        if self.conn:
            logging.info("Fechando conexão com o AlloyDB (Simulador)...")
            self.conn.commit()
            self.cursor.close()
            self.conn.close()

def run():
    source_conn_str = os.getenv("SOURCE_DB_CONN_STRING")
    alloydb_conn_str = os.getenv("ALLOYDB_CONN_STRING")
    source_table = "teste.tabelaTeste"
    target_table = "teste.tabelaTeste"

    with beam.Pipeline(options=PipelineOptions()) as p:
        # 1. Inicia a pipeline e lê do banco de origem
        data = (
            p 
            | 'Start' >> beam.Create([None]) # Cria um impulso inicial
            | 'ReadFromSourceDB' >> beam.ParDo(
                ReadFromSourceDB(source_conn_str, f"SELECT * FROM {source_table};")
            )
        )

        # 2. Loga os dados lidos (Validador 1)
        data | 'LogData' >> beam.Map(lambda x: logging.info(f"Lido da origem: {x}"))

        # 3. Escreve no banco de destino (Simulador do AlloyDB)
        write_results = data | 'WriteToAlloyDB' >> beam.ParDo(
            WriteToAlloyDB(alloydb_conn_str, target_table)
        )

        # 4. Loga o resultado da escrita (Validador 2)
        write_results | 'LogWriteStatus' >> beam.Map(
            lambda res: logging.info(f"Status da escrita para ID {res[0]}: {res[1]}")
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Iniciando a pipeline local do Apache Beam...")
    run()