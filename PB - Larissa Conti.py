import sqlite3
import pandas as pd
import json


#Criando as tabelas ================================================================================================================
def criar_tabela(nome_tabela, query):
    try:
        # Conectar ao banco de dados
        conn = sqlite3.connect('DataDB.db')
        cursor = conn.cursor()

        # Verificar se a tabela já existe
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{nome_tabela}';")
        if cursor.fetchone():
            print(f"\nTabela {nome_tabela} já existe.")
            print("\n=====================================================================")
        else:
            # Criar a tabela
            cursor.execute(query)
            print(f"\nTabela {nome_tabela} criada com sucesso!")
            print("\n=====================================================================")

        # Fechar a conexão com o banco de dados
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"\nErro ao criar a tabela {nome_tabela}: {e}")

#Tabela Funcionarios
query = ('''
CREATE TABLE IF NOT EXISTS Funcionarios (
    FuncionarioID INT PRIMARY KEY,
    Nome VARCHAR(100),
    CargoID INT,
    DepartamentoID INT,
    Salario REAL,
    Genero VARCHAR(20),
    FOREIGN KEY (CargoID) REFERENCES Cargos(CargoID),
    FOREIGN KEY (DepartamentoID) REFERENCES Departamentos(DepartamentoID)
);
               ''')
criar_tabela('Funcionarios', query)

#Tabela Cargos
query = ('''
CREATE TABLE IF NOT EXISTS Cargos (
    CargoID INT PRIMARY KEY,
    Descricao VARCHAR(100),
    SalarioBase REAL,
    Nivel VARCHAR(20),
    Beneficios VARCHAR(100)
);
               ''')
criar_tabela('Cargos', query)

#Tabela Departamentos
query = ('''
CREATE TABLE IF NOT EXISTS Departamentos (
    DepartamentoID INT PRIMARY KEY,
    NomeDepartamento VARCHAR(100),
    GerenteID INT,
    Andar INT,
    Funcoes TEXT,
    FOREIGN KEY (GerenteID) REFERENCES Funcionarios(FuncionarioID)
);
               ''')
criar_tabela('Departamentos', query)

#Tabela HistoricoSalarios
query = ('''
CREATE TABLE IF NOT EXISTS HistoricoSalarios (
    HistoricoSalarioID INT PRIMARY KEY,
    FuncionarioID INT,
    Data DATE,
    Salario REAL,
    FOREIGN KEY (FuncionarioID) REFERENCES Funcionarios(FuncionarioID)
);
               ''')
criar_tabela('HistoricoSalarios', query)

#Tabela Dependentes
query = ('''
CREATE TABLE IF NOT EXISTS Dependentes (
    DependenteID INT PRIMARY KEY,
    FuncionarioID INT,
    Nome VARCHAR(100),
    Genero VARCHAR(20),
    Idade INT,
    FOREIGN KEY (FuncionarioID) REFERENCES Funcionarios(FuncionarioID)
);
               ''')
criar_tabela('Dependentes', query)

# Tabela Projetos
query = (('''
CREATE TABLE IF NOT EXISTS Projetos (
    ProjetoID INT PRIMARY KEY,
    NomeProjeto TEXT NOT NULL,
    DescricaoProjeto TEXT,
    DataInicio DATE,
    DataConclusao DATE,
    FuncionarioResponsavelID INT,
    Custo REAL,
    Status TEXT,
    FOREIGN KEY (FuncionarioResponsavelID) REFERENCES Funcionarios(FuncionarioID)
);
                '''))
criar_tabela('Projetos', query)

# Tabela RecursosProjetos
query = (('''
CREATE TABLE IF NOT EXISTS RecursosProjetos (
    RecursosProjetoID INT PRIMARY KEY,
    ProjetoID INT,
    DescricaoRecurso TEXT,
    TipoRecurso TEXT,
    QuantidadeRecurso INTEGER,
    DataUtilizacao DATE,
    FOREIGN KEY (ProjetoID) REFERENCES Projetos(ProjetoID)
);
            '''))
criar_tabela('RecursosProjetos', query)


#Populando as tabelas criadas com arquivos CSV =======================================================================================
def inserir_dados(nome_tabela, arquivo_csv):
    try:
        # Conectar ao banco de dados
        conn = sqlite3.connect('DataDB.db')

        # Carregar dados do arquivo CSV para um DataFrame
        df_csv = pd.read_csv(arquivo_csv, delimiter=';', na_values='NULL')

        # Carregar dados existentes da tabela para um DataFrame
        df_db = pd.read_sql_query(f"SELECT * FROM {nome_tabela}", conn)

        # Salvar o número de linhas antes da inserção
        linhas_antes = len(df_db)

        # Concatenar os DataFrames e remover duplicatas
        df = pd.concat([df_db, df_csv]).drop_duplicates()

        # Salvar o número de linhas após a inserção
        linhas_depois = len(df)

        # Inserir dados na tabela do banco de dados usando to_sql
        df.to_sql(nome_tabela, conn, if_exists='replace', index=False)

        # Se o número de linhas aumentou, imprimir a mensagem de sucesso
        if linhas_depois > linhas_antes:
            print(f"\nDados inseridos na tabela {nome_tabela} com sucesso!")
        conn.close()

    except Exception as e:
        print(f"\nErro ao inserir dados na tabela {nome_tabela}: {e}")

# Inserir dados na tabela Funcionarios
inserir_dados('Funcionarios', 'funcionarios.csv')

# Inserir dados na tabela Cargos
inserir_dados('Cargos', 'cargos.csv')

# Inserir dados na tabela Departamentos
inserir_dados('Departamentos', 'departamento.csv')

# Inserir dados na tabela HistoricoSalarios
inserir_dados('HistoricoSalarios', 'historico_salarios.csv')

# Inserir dados na tabela Dependentes
inserir_dados('Dependentes', 'dependentes.csv')

#Inserir dados na tabela Projetos
inserir_dados('Projetos', 'projetos.csv')

#Inserir dados na tabela RecursosProjetos
inserir_dados('RecursosProjetos', 'recursos_projetos.csv')

#Consultas SQL ==========================================================================================================================

# Função para conectar ao banco de dados e executar uma consulta SQL
def executar_consulta_sql(query):
    conn = sqlite3.connect('DataDB.db')
    cursor = conn.cursor()
    cursor.execute(query)
    resultados = cursor.fetchall()
    colunas = [description[0] for description in cursor.description]
    conn.close()
    return [dict(zip(colunas, linha)) for linha in resultados]

# Função para salvar os resultados em um arquivo JSON
def salvar_em_json(nome_arquivo, dados):
    with open(nome_arquivo, 'w', encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)

# Consulta 1: Listar individualmente as tabelas em ordem crescente
def listar_tabela(tabela):
    query = f"SELECT * FROM {tabela} ORDER BY {tabela[:-1]}ID;"  #Removendo 's' para obter o nome da chave primária
    resultados = executar_consulta_sql(query)
    print(f"Conteúdo da tabela '{tabela}':")
    for linha in resultados:
        print(linha)
    print("\n=====================================================================")

# Consulta 2: Listar os funcionários com cargos, departamentos e os respectivos dependentes
def listar_funcionarios_com_info():
    query = """
        SELECT f.Nome AS Funcionario, c.Descricao AS Cargo, d.NomeDepartamento, dep.Nome AS Dependente
        FROM Funcionarios f
        JOIN Cargos c ON f.CargoID = c.CargoID
        JOIN Departamentos d ON f.DepartamentoID = d.DepartamentoID
        LEFT JOIN Dependentes dep ON f.FuncionarioID = dep.FuncionarioID;
        """
    resultados = executar_consulta_sql(query)
    print("Funcionários com informações completas (Nome, Cargo, Departamento, Dependente):")
    for linha in resultados:
        print(linha)
    print("\n=====================================================================")
    

# Consulta 3: Listar os funcionários que tiveram aumento salarial nos últimos 3 meses
def listar_funcionarios_com_aumento():
    query = """
      SELECT f.Nome
      FROM Funcionarios f
      JOIN HistoricoSalarios hs ON f.FuncionarioID = hs.FuncionarioID
      WHERE hs.Data BETWEEN '2023-01' AND '2023-06'
      GROUP BY f.Nome
      HAVING MIN(hs.Salario) != MAX(hs.Salario);
      """
    resultados = executar_consulta_sql(query)
    print("Funcionários com aumento salarial nos últimos 3 meses:")
    for linha in resultados:
        print(linha)
    print("\n=====================================================================")

# Consulta 4: Listar a média de idade dos filhos dos funcionários por departamento
def listar_media_idade_filhos_por_departamento():
    query = """
       SELECT d.NomeDepartamento, ROUND(AVG(dep.Idade)) AS MediaIdade
       FROM Departamentos d
       JOIN Funcionarios f ON d.DepartamentoID = f.DepartamentoID
       JOIN Dependentes dep ON f.FuncionarioID = dep.FuncionarioID
       GROUP BY d.NomeDepartamento;
       """
    resultados = executar_consulta_sql(query) 
    print("Média de idade dos filhos por departamento:")
    for linha in resultados:
        print(linha)
    print("\n=====================================================================")

# Consulta 5: Listar qual estagiário possui filho
def listar_estagiarios_com_filho():
    query = """
        SELECT DISTINCT f.Nome
        FROM Funcionarios f
        JOIN Cargos c ON f.CargoID = c.CargoID
        JOIN Dependentes dep ON f.FuncionarioID = dep.FuncionarioID
        WHERE c.Descricao = 'Estagiário';
        """
    resultados = executar_consulta_sql(query)
    print("Estagiários que possuem filho:")
    for linha in resultados:
        print(linha)
    print("\n=====================================================================")
    
#Consulta 1 TP 4: Listar a média dos salários dos funcionários responsáveis por projetos concluídos, agrupados por departamento
def listar_media_salarios_funcionarios_projetos_concluidos():
    query = """
        SELECT d.NomeDepartamento AS Departamento, AVG(f.Salario) AS MediaSalarios
        FROM Funcionarios f
        JOIN Projetos p ON f.FuncionarioID = p.FuncionarioResponsavelID
        JOIN Departamentos d ON f.DepartamentoID = d.DepartamentoID
        WHERE p.Status = 'Concluído'
        GROUP BY d.NomeDepartamento;
        """
    resultados = executar_consulta_sql(query)
    print("Média dos salários dos funcionários responsáveis por projetos concluídos, agrupados por departamento:")
    for linha in resultados:
        print(linha)
    print("\n=====================================================================")
    return resultados
    
#Consulta 2 TP4: Identificar os três recursos materiais mais usados nos projetos, listando a descrição do recurso e a quantidade total usada
def listar_recursos_mais_utilizados():
    query = """
        SELECT DescricaoRecurso, SUM(QuantidadeRecurso) AS TotalUtilizado
        FROM RecursosProjetos
        WHERE TipoRecurso = 'Material'
        GROUP BY DescricaoRecurso
        ORDER BY TotalUtilizado DESC
        LIMIT 3;
        """
    resultados = executar_consulta_sql(query)
    print("Recursos materiais mais utilizados nos projetos:")
    for linha in resultados:
        print(linha)
    print("\n=====================================================================")
    return resultados
    
#Consulta 3 TP4: Calcular o custo total dos projetos por departamento, considerando apenas os projetos 'Concluídos'
def listar_custo_total_projetos_concluidos_por_departamento():
    query = """
        SELECT d.NomeDepartamento AS Departamento, SUM(p.Custo) AS CustoTotal
        FROM Projetos p
        JOIN Funcionarios f ON p.FuncionarioResponsavelID = f.FuncionarioID
        JOIN Departamentos d ON f.DepartamentoID = d.DepartamentoID
        WHERE p.Status = 'Concluído'
        GROUP BY d.NomeDepartamento;
        """
    resultados = executar_consulta_sql(query)
    print("Custo total dos projetos concluídos por departamento:")
    for linha in resultados:
        print(linha)
    print("\n=====================================================================")
    return resultados
    
#Consulta 4 TP4: Listar todos os projetos com seus respectivos nomes, custo, data de início, data de conclusão e o nome do funcionário responsável, que estejam 'Em Execução'
def listar_projetos_em_execucao():
    query = """
        SELECT p.NomeProjeto, p.Custo, p.DataInicio, p.DataConclusao, f.Nome AS FuncionarioResponsavel
        FROM Projetos p
        JOIN Funcionarios f ON p.FuncionarioResponsavelID = f.FuncionarioID
        WHERE p.Status = 'Em Execução';
        """
    resultados = executar_consulta_sql(query)
    print("Projetos em execução: Nome do Projeto, Custo, Data de Início, Data de Conclusão e Funcionário Responsável:" )
    for linha in resultados:
        print(linha)
    print("\n=====================================================================")

#Consulta 5 TP4: Identificar o projeto com o maior número de dependentes envolvidos, considerando que os dependentes são associados aos funcionários que estão gerenciando os projetos
def listar_projeto_maior_numero_dependentes():
    query = """
        SELECT p.NomeProjeto, COUNT(dep.DependenteID) AS NumeroDependentes
        FROM Projetos p
        JOIN Funcionarios f ON p.FuncionarioResponsavelID = f.FuncionarioID
        JOIN Dependentes dep ON f.FuncionarioID = dep.FuncionarioID
        GROUP BY p.NomeProjeto
        ORDER BY NumeroDependentes DESC
        LIMIT 1;
        """
    resultados = executar_consulta_sql(query)
    print("Projeto com o maior número de dependentes envolvidos:")
    for linha in resultados:
        print(linha)
    print("\n=====================================================================")


# Estabelecer conexão com o banco de dados
conn = sqlite3.connect('empresa.db')

# Executar as consultas
listar_tabela('Funcionarios') #Query 1
listar_tabela('Cargos')
listar_tabela('Departamentos')
listar_tabela('HistoricoSalarios')
listar_tabela('Dependentes')

listar_funcionarios_com_info() #Query 2
listar_funcionarios_com_aumento() #Query 3
listar_media_idade_filhos_por_departamento() #Query 4
listar_estagiarios_com_filho() #Query 5
listar_media_salarios_funcionarios_projetos_concluidos() #Query 1 TP 4
listar_recursos_mais_utilizados() #Query 2 TP 4
listar_custo_total_projetos_concluidos_por_departamento() #Query 3 TP 4
listar_projetos_em_execucao() #Query 4 TP 4
listar_projeto_maior_numero_dependentes() #Query 5 TP 4

# Fechar conexão com o banco de dados
conn.close()

# Executar as consultas e salvar os resultados em arquivos JSON (Queries 1, 2, 3 TP4)
media_salarios = listar_media_salarios_funcionarios_projetos_concluidos()
salvar_em_json('media_salarios_projetos_concluidos.json', media_salarios)

recursos_mais_utilizados = listar_recursos_mais_utilizados()
salvar_em_json('recursos_mais_utilizados.json', recursos_mais_utilizados)

custo_total_projetos = listar_custo_total_projetos_concluidos_por_departamento()
salvar_em_json('custo_total_projetos_concluidos.json', custo_total_projetos)

#Queries com manipulação de arquivo CSV =================================================================================================

#Preparando os Dataframes
df_historico_salarios = pd.read_csv('historico_salarios.csv', sep=';', decimal=',')
df_cargos = pd.read_csv('cargos.csv', sep=';', decimal=',')
df_departamentos = pd.read_csv('departamento.csv', sep=';', decimal=',')
df_funcionarios = pd.read_csv('funcionarios.csv', sep=';', decimal=',')
df_dependentes = pd.read_csv('dependentes.csv', sep=';', decimal=',')

#Consulta 6 - Listar o funcionário com o salário médio mais alto
def listar_funcionario_com_maior_salario_medio():
    media_salarios = df_historico_salarios.groupby('FuncionarioID')['Salario'].mean()
    funcionario_com_maior_salario_medio = media_salarios.idxmax() - 1 #Acertando o índice para corresponder ao FuncionarioID
    nome_funcionario = df_funcionarios.loc[funcionario_com_maior_salario_medio, 'Nome']
    print(f'O funcionário com o maior salário médio é {nome_funcionario}, com um salário médio de R${media_salarios.max():.2f}.')
    print("\n=====================================================================")


#Consulta 7 - Listar o analista que é pai de duas meninas
def listar_analistas_com_duas_filhas():
    
    # Filtra funcionários que são analistas homens
    analistas_homens = df_funcionarios[
        (df_funcionarios["CargoID"] == 3) & (df_funcionarios["Genero"] == "Masculino")
    ]

    # Filtra dependentes que são filhas do gênero feminino
    filhas = df_dependentes[(df_dependentes["Genero"] == "Feminino")]

    # Junta os DataFrames de analistas homens e filhas com base no FuncionarioID
    analistas_com_filhas = analistas_homens.merge(filhas, on="FuncionarioID")

    # Agrupa por FuncionarioID e conta o número de filhas
    contagem_filhas = analistas_com_filhas.groupby("FuncionarioID")["DependenteID"].count()

    # Filtra analistas com exatamente duas filhas
    analistas_com_duas_filhas = contagem_filhas[contagem_filhas == 2].index.tolist()

    # Filtra o DataFrame original de funcionários para incluir apenas os analistas com duas filhas
    resultado = df_funcionarios[df_funcionarios["FuncionarioID"].isin(analistas_com_duas_filhas)]
    
    print(f"Analistas que são pais de duas filhas: {resultado['Nome'].tolist()}")
    print("\n=====================================================================")
   
   
#Consulta 8 - Listar o analista com o salário mais alto e que ganhe entre R$ 5000 e R$ 9000    
def listar_analista_salario_mais_alto():

    # Filtra funcionários que são analistas e estão na faixa salarial desejada
    analistas_faixa_salarial = df_funcionarios[
        (df_funcionarios["CargoID"] == 3) & (df_funcionarios["Salario"] >= 5000) & (df_funcionarios["Salario"] <= 9000)
    ]

    # Encontra o analista com o salário mais alto
    analista_salario_mais_alto = analistas_faixa_salarial.loc[analistas_faixa_salarial["Salario"].idxmax()]

    # Retorna o nome e salário do analista
    resultado = analista_salario_mais_alto[["Nome", "Salario"]]
    
    print(f"Analista com o salário mais alto entre R$ 5000 e R$ 9000: {resultado['Nome']} com salário de R${resultado['Salario']:.2f}")
    print("\n=====================================================================")
    
#Consulta 9 - Listar qual departamento possui o maior número de dependentes
def listar_departamento_mais_dependentes():
    # Junta os DataFrames de funcionários e dependentes com base no FuncionarioID
    funcionarios_com_dependentes = df_funcionarios.merge(df_dependentes, on="FuncionarioID", how="left")

    # Junta o DataFrame resultante com o DataFrame de departamentos com base no DepartamentoID
    funcionarios_dependentes_departamentos = funcionarios_com_dependentes.merge(df_departamentos, on="DepartamentoID", how="left")

    # Agrupa por departamento e conta o número de dependentes
    contagem_dependentes = funcionarios_dependentes_departamentos.groupby("NomeDepartamento")["DependenteID"].count()

    # Encontra o departamento com o maior número de dependentes
    departamento_mais_dependentes = contagem_dependentes.idxmax()
    numero_dependentes = contagem_dependentes.max()

    # Retorna o nome do departamento e o número de dependentes
    print(f"O departamento com o maior número de dependentes é o {departamento_mais_dependentes}, com {numero_dependentes} dependentes.")
    print("\n=====================================================================")

#Consulta 10 - Listar a média de salário por departamento, em ordem decrescente
def listar_media_salarial_por_departamento():
    
    # Junta os DataFrames de funcionários e departamentos com base no DepartamentoID
    funcionarios_departamentos = df_funcionarios.merge(df_departamentos, on="DepartamentoID")

    # Calcula a média salarial por departamento e ordena em ordem decrescente
    media_salarial = funcionarios_departamentos.groupby("NomeDepartamento")["Salario"].mean().sort_values(ascending=False)
    
    #Oraganizando o resultado em um DataFrame e nomeando corretamente as colunas
    df_resultado = pd.DataFrame({'Departamento': media_salarial.index, 'Média Salarial': media_salarial.values})

    print("Média salarial por departamento (em ordem decrescente):")
    print(df_resultado)
    print("\n=====================================================================")

listar_funcionario_com_maior_salario_medio() #Query 6
listar_analistas_com_duas_filhas() #Query 7
listar_analista_salario_mais_alto() #Query 8
listar_departamento_mais_dependentes() #Query 9
listar_media_salarial_por_departamento() #Query 10



