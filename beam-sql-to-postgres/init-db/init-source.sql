-- Cria o schema e a tabela de origem
CREATE SCHEMA IF NOT EXISTS teste;

CREATE TABLE teste.tabelaTeste (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100),
    categoria VARCHAR(50),
    valor NUMERIC(10, 2),
    data_criacao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insere alguns dados de exemplo
INSERT INTO teste.tabelaTeste (nome, categoria, valor) VALUES
('Produto A', 'Eletrônicos', 1250.75),
('Produto B', 'Livros', 99.90),
('Produto C', 'Eletrônicos', 3499.00),
('Produto D', 'Casa', 250.00);

-- Cria o schema no banco de destino (para a tabela ser criada depois)
-- Embora este script rode no banco de origem, teremos que garantir que o schema exista no de destino.
-- A pipeline criará a tabela se não existir.