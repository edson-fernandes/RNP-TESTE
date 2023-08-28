
1. Inicialização do Spark:
Objetivo: Criar uma sessão do Spark para permitir operações como leitura/escrita de dados e execução de SQL.
Funções Importantes:
SparkSession.builder: Configura e constrói uma nova sessão Spark.
.appName(): Define o nome da aplicação Spark.
.getOrCreate(): Obtém a sessão Spark existente ou, se não houver, cria uma nova.
2. Configuração da Conexão com o PostgreSQL:
Objetivo: Armazenar as informações necessárias para se conectar ao banco de dados PostgreSQL.
Variáveis Principais:
database_url: Contém a URL do banco de dados.
propriedades: Um dicionário que mantém informações do usuário, senha e driver JDBC necessário para a conexão.
3. Leitura de Tabelas:
Objetivo: Ler tabelas específicas do banco de dados PostgreSQL e armazená-las como DataFrames do Spark.
Função: read_table(table_name): Esta função recebe o nome de uma tabela como argumento e retorna um DataFrame correspondente a essa tabela.
.jdbc(): Função usada para ler dados de um banco de dados usando JDBC.
4. Exibição dos Dados:
Objetivo: Verificar se os dados foram carregados corretamente.
Função:
.show(): Exibe os primeiros registros de um DataFrame.
5. Salvar Dados em Formato Parquet:
Objetivo: Armazenar os DataFrames em um formato eficiente e colunar chamado Parquet.
Função: save_as_parquet(df, table_name): Salva o DataFrame fornecido no formato Parquet no caminho especificado.
.write.parquet(): Método usado para escrever um DataFrame como um arquivo Parquet.
6. Criação de Tabelas Temporárias:
Objetivo: Transformar os DataFrames em tabelas temporárias, permitindo consultas SQL diretas.
Função:
.createOrReplaceTempView(): Registra o DataFrame como uma tabela temporária no Spark, permitindo consultas SQL diretamente nela.
7. Consultas SQL:
Objetivo: Extrair informações específicas dos DataFrames usando a linguagem SQL.
Função:
spark.sql(): Permite executar consultas SQL diretamente nos DataFrames registrados como tabelas temporárias.
8. Upsert de Tabelas:
Objetivo: Combinar dados de duas fontes (JDBC e Parquet), identificar diferenças e criar um DataFrame unificado.
Funções:
upsert_table(): Realiza operações de junção para identificar novos, atualizados e registros excluídos e combina-os em um único DataFrame.
.withColumn(): Adiciona uma nova coluna ou substitui uma coluna existente em um DataFrame.
.join(): Realiza operações de junção entre dois DataFrames.
9. Salvando DataFrames Merged:
Objetivo: Persistir os DataFrames consolidados em formato Delta para futuras operações.
Funções:
.write.format("delta"): Especifica o formato Delta para escrita.
.mode("overwrite"): Especifica o modo de gravação (neste caso, sobrescrever se o arquivo existir).
.save(): Grava o DataFrame no caminho especificado.
Resumindo: Este script PySpark é uma solução completa para extrair dados de um banco de dados PostgreSQL, realizar análises e consultas, e então combinar ("upsert") os resultados com dados armazenados em formato Parquet, finalizando com a persistência dos DataFrames consolidados em formato Delta.
