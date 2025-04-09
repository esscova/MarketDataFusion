# bibliotecas
import logging
from etl import Dados 

# configs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    path_json = '../data/raw/dados_empresaA.json'
    path_csv = '../data/raw/dados_empresaB.csv'
    path_dados_combinados = '../data/processed/dados_combinados_via_etl.csv'

    key_mapping = {
        'Nome do Item': 'Nome do Produto',
        'Classificação do Produto': 'Categoria do Produto',
        'Valor em Reais (R$)': 'Preço do Produto (R$)',
        'Quantidade em Estoque': 'Quantidade em Estoque',
        'Nome da Loja': 'Filial',
        'Data da Venda': 'Data da Venda'
    }

    # --- extract
    logging.info("[E] Extraindo dados e criando objetos Dados...")
    dados_empresaA = Dados.leitura_dados(path_json, 'json')
    logging.info(f"  - Empresa A: {dados_empresaA.qtd_linhas} registros lidos. Colunas: {dados_empresaA.nome_colunas}")

    dados_empresaB = Dados.leitura_dados(path_csv, 'csv')
    logging.info(f"  - Empresa B: {dados_empresaB.qtd_linhas} registros lidos. Colunas Iniciais: {dados_empresaB.nome_colunas}")

    # --- transform
    logging.info("[T] Transformando dados...")
    dados_empresaB.rename_columns(key_mapping)
    logging.info(f"  - Empresa B - Colunas após renomear: {dados_empresaB.nome_colunas}")

    dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
    logging.info(f"  - Dados combinados: {dados_fusao.qtd_linhas} registros totais.")
    logging.info(f"  - Colunas finais para CSV: {dados_fusao.nome_colunas}")

    # --- load
    logging.info("[L] Carregando dados...")
    dados_fusao.salvando_dados(path_dados_combinados)

    logging.info("--- PIPELINE CONCLUÍDO ---")

if __name__ == '__main__':
    main()