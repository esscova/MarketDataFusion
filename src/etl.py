# A classe Dados implementa uma pipeline ETL com as seguintes funcionalidades principais:

# Leitura de Dados: O método de classe leitura_dados lê arquivos JSON ou CSV e retorna uma instância de Dados.
# Transformação de Dados: O método rename_columns permite renomear colunas dos dados.
# Junção de Dados: O método de classe join combina os dados de duas instâncias de Dados.
# Salvamento de Dados: O método salvando_dados salva os dados processados em um arquivo CSV.

# ----

# bibliotecas
import json
import csv
import logging

# configs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# classe etl
class Dados:
    """
    Representa um conjunto de dados (lido de JSON ou CSV) e encapsula
    as operações de ETL (leitura, transformação, salvamento) para o pipeline.

    Atributos Principais:
        dados (list): A lista de dicionários contendo os registros.
        nome_colunas (list): Os nomes das colunas identificados nos dados.
        qtd_linhas (int): O número total de registros (linhas) nos dados.
    """
    def __init__(self, dados):
        """
        Construtor da classe. Chamado internamente por `leitura_dados` ou `join`.
        Inicializa os atributos da instância (self).

        Args:
            dados (list): A lista de dicionários lida do arquivo ou combinada.
        """

        logging.debug("Inicializando nova instância de Dados...")

        self.dados = dados
        self.nome_colunas = self.__get_columns()
        logging.debug(f"Colunas identificadas: {self.nome_colunas}")

        self.qtd_linhas = self.__size_data()
        logging.debug(f"Quantidade de linhas identificada: {self.qtd_linhas}")

    @classmethod
    def leitura_dados(cls, path, tipo_dados):
        """
        Método de Fábrica: Lê dados de um arquivo (JSON ou CSV) e
        RETORNA UMA NOVA INSTÂNCIA da classe Dados (cls)
        contendo esses dados.

        Args:
            cls: Referência à própria classe (Dados). Passado automaticamente.
            path (str): O caminho para o arquivo a ser lido.
            tipo_dados (str): O tipo do arquivo ('json' ou 'csv').

        Returns:
            Dados: Uma nova instância da classe Dados, ou uma instância vazia se houver erro.
        """

        dados = []
        logging.info(f"Iniciando leitura do arquivo: {path} (tipo: {tipo_dados})")

        try: 
            if tipo_dados.lower() == 'csv':
                with open(path, 'r', encoding='utf-8') as file:
                    spamreader = csv.DictReader(file, delimiter=',')
                    dados = list(spamreader)
                    logging.debug(f"Lidos {len(dados)} registros do CSV.") 

            elif tipo_dados.lower() == 'json':
                with open(path, 'r', encoding='utf-8') as file:
                    dados = json.load(file)
                    logging.debug(f"Lidos {len(dados)} registros do JSON.") 

            else:
                logging.error(f"Tipo de arquivo não suportado: {tipo_dados}")
                return cls([]) # lista vazia para nao quebrar

        except FileNotFoundError:
            logging.error(f"Erro: Arquivo não encontrado em {path}")
            return cls([])
        except json.JSONDecodeError:
            logging.error(f"Erro: Falha ao decodificar JSON em {path}")
            return cls([]) 
        except Exception as e:
            logging.exception(f"Erro inesperado ao ler o arquivo {path}: {e}")
            return cls([]) 
        
        logging.info(f"Leitura do arquivo {path} concluída com sucesso.")
        return cls(dados)
    
    def __get_columns(self):
        """Método interno ('privado') para extrair nomes das colunas."""
        if not self.dados:
            logging.warning("Tentativa de obter colunas de um conjunto de dados vazio.")
            return []

        return list(self.dados[-1].keys())

    def __size_data(self):
        """Método interno ('privado') para calcular o número de registros."""
        return len(self.dados)

    def __transformando_dados_tabela(self):
        """
        Método interno ('privado') para converter a lista de dicionários
        (self.dados) em formato de tabela (lista de listas),
        preenchendo colunas ausentes com 'Indisponivel'.
        Usado apenas antes de salvar em CSV.
        """
        if not self.dados:
            logging.warning("Tentativa de transformar dados vazios para tabela.")
            return []

        if not self.nome_colunas and self.dados:
             self.nome_colunas = self.__get_columns()
        
        if not self.nome_colunas: 
             logging.warning("Não foi possível determinar colunas para transformar em tabela.")
             return []

        dados_combinados_tabela = [self.nome_colunas]

        for row_dict in self.dados: 
            linha = []

            for coluna in self.nome_colunas: 
                linha.append(row_dict.get(coluna, 'Indisponivel'))
            
            dados_combinados_tabela.append(linha)

        logging.debug("Dados transformados para formato de tabela (lista de listas).")
        return dados_combinados_tabela
    def rename_columns(self, key_mapping):
        """
        Renomeia as colunas (chaves dos dicionários) nos dados da instância (self.dados)
        usando o dicionário 'key_mapping' fornecido.
        Modifica diretamente os atributos self.dados e self.nome_colunas.
        """
        logging.info(f"Iniciando renomeação de colunas...")
        
        if not self.dados:
            logging.warning("Não há dados para renomear.")
            return 

        if not isinstance(key_mapping, dict):
             logging.error("Erro: key_mapping fornecido para rename_columns não é um dicionário.")
             return 

        new_dados = []
        
        for old_dict in self.dados:
            if not isinstance(old_dict, dict): continue # Ignora se não for dicionário
            dict_temp = {}
        
            for old_key, value in old_dict.items():
                new_key = key_mapping.get(old_key, old_key)
                dict_temp[new_key] = value
            new_dados.append(dict_temp)

        self.dados = new_dados
        self.nome_colunas = self.__get_columns()
        logging.info("Renomeação de colunas concluída.")
        logging.debug(f"Novas colunas após renomeação: {self.nome_colunas}")

    def salvando_dados(self, path):
        """
        Salva os dados da instância (self.dados) em um arquivo CSV no 'path' especificado.
        Internamente, converte para formato de tabela antes de salvar.
        """
        logging.info(f"Iniciando salvamento dos dados em: {path}")
        dados_para_salvar = self.__transformando_dados_tabela()

        if not dados_para_salvar: 
            logging.error("Falha ao transformar dados para tabela. Salvamento abortado.")
            return

        try:
            with open(path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerows(dados_para_salvar)

            logging.info(f"Dados salvos com sucesso em: {path}")

        except Exception as e:
            logging.exception(f"Erro ao salvar o arquivo em {path}: {e}")
    @classmethod
    def join(cls, dadosA, dadosB):
        """
        Método de Classe: Combina os dados de duas instâncias Dados (dadosA, dadosB)
        e retorna uma NOVA instância Dados com os dados combinados.

        Args:
            cls: A classe Dados.
            dadosA (Dados): A primeira instância de Dados a ser combinada.
            dadosB (Dados): A segunda instância de Dados a ser combinada.

        Returns:
            Dados: Uma nova instância de Dados contendo a fusão, ou instância vazia se erro.
        """
        logging.info("Iniciando a junção de dois conjuntos de dados...")
        combined_list = []

        if isinstance(dadosA, cls) and hasattr(dadosA, 'dados'):
            combined_list.extend(dadosA.dados)
            logging.debug(f"Adicionados {len(dadosA.dados)} registros de dadosA.")
        else:
            logging.warning("dadosA fornecido para join não é uma instância válida de Dados ou não possui atributo 'dados'.")

        if isinstance(dadosB, cls) and hasattr(dadosB, 'dados'):
            combined_list.extend(dadosB.dados)
            logging.debug(f"Adicionados {len(dadosB.dados)} registros de dadosB.")
        else:
            logging.warning("dadosB fornecido para join não é uma instância válida de Dados ou não possui atributo 'dados'.")

        if not combined_list:
            logging.warning("Junção resultou em lista vazia.")
            return cls([]) 

        logging.info(f"Junção concluída. Total de {len(combined_list)} registros combinados.")
        return cls(combined_list)
