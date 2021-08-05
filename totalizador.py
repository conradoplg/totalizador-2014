import json
from collections import namedtuple
import time
import os.path
from multiprocessing import Pool

from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import psycopg2


def index():
    """
    Cria index.json com todos estados, municípios, zonas e seções.

    Navega pelo menu usando selenium e chromedriver.

    O index.json já foi incluído no repositório, não é necessário regerá-lo
    a não ser que queira verificá-lo.
    """
    driver = webdriver.Chrome(executable_path='./chromedriver')
    driver.get("http://inter04.tse.jus.br/ords/eletse/f?p=20103:1:::NO:::")

    turno_select = Select(driver.find_element_by_id('P0_X_TURNO'))
    assert turno_select
    turno_select.select_by_visible_text('2')

    uf_select = Select(driver.find_element_by_id('P0_X_UF'))
    uf_list = [option.get_attribute('value') for option in uf_select.options][1:]
    uf_list.sort()
    data = {}

    for uf in uf_list:
        uf_select = Select(driver.find_element_by_id('P0_X_UF'))
        uf_select.select_by_value(uf)
        municipio_select = Select(driver.find_element_by_id('P0_X_MUN'))
        municipio_list = [option.get_attribute('value') for option in municipio_select.options][1:]
        municipio_list.sort()
        data[uf] = {}

        for municipio in municipio_list:
            municipio_select = Select(driver.find_element_by_id('P0_X_MUN'))
            municipio_select.select_by_value(municipio)
            zona_select = Select(driver.find_element_by_id('P0_X_ZONA'))
            zona_list = [option.get_attribute('value') for option in zona_select.options][1:]
            zona_list.sort()
            data[uf][municipio] = {}

            for zona in zona_list:
                zona_select = Select(driver.find_element_by_id('P0_X_ZONA'))
                zona_select.select_by_value(zona)
                secao_select = Select(driver.find_element_by_id('P0_X_SECAO'))
                secao_list = [option.get_attribute('value') for option in secao_select.options][1:]
                secao_list.sort()
                data[uf][municipio][zona] = secao_list

    fn = 'index.json'
    with open(fn, 'w') as f:
        json.dump(data, f)
    print('Wrote ' + fn)

    driver.close()


def stats():
    """
    Retorna número de estados, municípios, zonas e seções.
    """
    with open('index.json', 'r') as f:
        data = json.load(f)
    total_uf = 0
    total_municipio = 0
    total_zona = 0
    total_secao = 0
    for uf in data:
        for municipio in data[uf]:
            for zona in data[uf][municipio]:
                total_secao += len(data[uf][municipio][zona])
                total_zona += 1
            total_municipio += 1
        total_uf += 1
    return total_uf, total_municipio, total_zona, total_secao


def get_bu(driver, uf, municipio, zona, secao):
    """Retorna um BU (HTML).

    Args:
        driver: driver selenium pré-criado
        uf (str): sigla do estado
        municipio (str): código numérico do município
        zona (str): código numérico da zona
        secao (str): código numérico da seção.

    Returns:
        bytes: HTML do BU
    """
    turno_select = Select(driver.find_element_by_id('P0_X_TURNO'))
    turno_select.select_by_visible_text('2')

    uf_select = Select(driver.find_element_by_id('P0_X_UF'))
    uf_select.select_by_value(uf)
    municipio_select = Select(driver.find_element_by_id('P0_X_MUN'))
    municipio_select.select_by_value(municipio)
    zona_select = Select(driver.find_element_by_id('P0_X_ZONA'))
    zona_select.select_by_value(zona)
    secao_select = Select(driver.find_element_by_id('P0_X_SECAO'))
    secao_select.select_by_value(secao)

    pesquisar_button = driver.find_element_by_link_text('Pesquisar')
    pesquisar_button.click()

    source = driver.page_source

    return source


def extract_bu_data(source):
    """Retorna dados do BU.

    Args:
        source (bytes): o HTML do BU

    Returns:
        tuple(int, int, int, int, int): Votos da Dilma, Aécio, brancos, nulos e faltas.
            Se a seção não foi apurado, retorna todos os valores 0.
    """
    soup = BeautifulSoup(source, 'html.parser')

    apurada_tag = soup.find("td", string='Apurada')
    if not apurada_tag:
        return (0, 0, 0, 0, 0)

    faltas_tag = soup.find("th", string='Eleitores Faltosos')
    faltas = int(faltas_tag.find_next_sibling('td').text)

    dilma_tag = soup.find("td", string='DILMA')
    dilma = int(dilma_tag.find_next_siblings('td')[1].text) if dilma_tag else 0
    aecio_tag = soup.find("td", string='AÉCIO NEVES')
    aecio = int(aecio_tag.find_next_siblings('td')[1].text) if aecio_tag else 0

    tag = dilma_tag if dilma_tag else aecio_tag
    if not tag:
        return (dilma, aecio, 0, 0, faltas)
    brancos_tag = tag.find_next("th", string='Brancos')
    brancos = int(brancos_tag.find_next_sibling('td').text)
    nulos_tag = tag.find_next("th", string='Nulos')
    nulos = int(nulos_tag.find_next_sibling('td').text)
    return (dilma, aecio, brancos, nulos, faltas)


def test_extract_bu_data():
    """Testa extração de dados do BU.
    """
    with open('test.html', 'r') as f:
        source = f.read()
    r = extract_bu_data(source)
    assert r[0] == 99
    assert r[1] == 153
    assert r[2] == 3
    assert r[3] == 8
    assert r[4] == 34


def download_zona_bus(data, secao_total, uf, municipio, zona):
    """Faz download de todos os BUs de uma zona.

    Os BUs são salvos em "bu/<UF>-<municipio>-<zona>-<secao>.html".
    É criado um arquivo vazio "bu/<UF>-<municipio>-<zona>.done" para indicar
    a conclusão do processo.

    Args:
        data (dict): dicionário carregado do index.json
        secao_total (int): número de seções da zona.
        uf (str): sigla do estado
        municipio (str): código numérico do município
        zona (str): código numérico da zona
    """
    print('Downloading {}-{}-{}'.format(uf, municipio, zona))
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(executable_path='./chromedriver', options=chrome_options)
    driver.get("http://inter04.tse.jus.br/ords/eletse/f?p=20103:1:::NO:::")
    start_time = time.monotonic()
    secao_processed = 0
    for secao in data[uf][municipio][zona]:
        source = get_bu(driver, uf, municipio, zona, secao)
        fn = 'bu/{}-{}-{}-{}.html'.format(uf, municipio, zona, secao)
        with open(fn, 'w') as f:
            f.write(source)
        secao_processed += 1
    fn = 'bu/{}-{}-{}.done'.format(uf, municipio, zona)
    with open(fn, 'w'):
        pass
    time_left = ((time.monotonic() - start_time) / float(secao_processed)) * float(secao_total)
    print('Done {}-{}-{}; {}/{}; {:.2f} s left'.format(uf, municipio, zona, secao_processed, secao_total, time_left))
    driver.close()


def download_all_bus():
    """Faz download de todos os BUs.

    Pode ser interrompido; ele continuará de onde parou.
    """
    with open('index.json', 'r') as f:
        data = json.load(f)
    secao_total = stats()[3]

    def iter_zonas():
        for uf in data:
            for municipio in data[uf]:
                for zona in data[uf][municipio]:
                    fn = 'bu/{}-{}-{}.done'.format(uf, municipio, zona)
                    if os.path.isfile(fn):
                        print('Skipping', fn)
                    else:
                        yield (data, secao_total, uf, municipio, zona)

    with Pool(16) as p:
        p.starmap(download_zona_bus, iter_zonas())


def sum_zona_bus(params):
    """Processa BUs de uma zona, inserindo em banco de dados cada BU,
    e retorna soma de votos.

    Args:
        params (tuple(dict, str, str, str)): índice, estado, município, zona

    Returns:
        tuple(str, str, str, tuple(int, int, int, int, int)): estado, município, zona, soma
        de votos (ver extract_bu_data).
    """
    with psycopg2.connect("") as conn:
        with conn.cursor() as cur:

            (data, uf, municipio, zona) = params
            votos = (0, 0, 0, 0, 0)
            for secao in data[uf][municipio][zona]:
                fn = 'bu/{}-{}-{}-{}.html'.format(uf, municipio, zona, secao)
                with open(fn, 'r') as f:
                    source = f.read()
                # print(fn)
                try:
                    new_votos = extract_bu_data(source)
                except:
                    print(fn)
                    raise
                cur.execute('''
                    INSERT INTO presidente_2014_2 (uf, municipio, zona, secao, dilma, aecio, brancos, nulos, faltas)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (uf, municipio, zona, secao, new_votos[0], new_votos[1], new_votos[2], new_votos[3], new_votos[4]))
                votos = tuple(v1 + v2 for v1, v2 in zip(new_votos, votos))
        conn.commit()
        return uf, municipio, zona, votos


def sum_all_bus():
    """Processa todos os BUs, inserindo dados de cada seção em banco de dados e
    mostrando o total.
    """
    with psycopg2.connect("") as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS presidente_2014_2;")
            cur.execute('''CREATE TABLE IF NOT EXISTS presidente_2014_2 (
                uf varchar, municipio varchar, zona varchar, secao varchar,
                dilma integer, aecio integer, brancos integer, nulos integer, faltas integer,
                PRIMARY KEY (uf, municipio, zona, secao)
            );
            ''')
        conn.commit()

    with open('index.json', 'r') as f:
        data = json.load(f)
    total_uf, total_municipio, total_zona, total_secao = stats()

    def iter_zonas():
        for uf in data:
            for municipio in data[uf]:
                for zona in data[uf][municipio]:
                    fn = 'bu/{}-{}-{}.done'.format(uf, municipio, zona)
                    if os.path.isfile(fn):
                        yield (data, uf, municipio, zona)

    start_time = time.monotonic()
    secao_processed = 0
    votos = (0, 0, 0, 0, 0)

    with Pool(24) as p:
        for (uf, municipio, zona, new_votos) in p.imap(sum_zona_bus, iter_zonas()):
            secao_size = len(data[uf][municipio][zona])
            votos = tuple(v1 + v2 for v1, v2 in zip(votos, new_votos))
            secao_processed += secao_size
            time_left = ((time.monotonic() - start_time) / float(secao_processed)) * float(total_secao - secao_processed)
            print(votos)
            print('{}/{}; {:.2f} s left'.format(secao_processed, total_secao, time_left))


if __name__ == '__main__':
    # driver = webdriver.Chrome(executable_path='./chromedriver')
    # driver.get("http://inter04.tse.jus.br/ords/eletse/f?p=20103:1:::NO:::")
    # source = get_bu(driver, 'AC', "01007", "9", "1")
    # driver.close()
    # test_extract_bu_data()
    # print(stats())
    # download_all_bus()

    # Não fiz processamento de argumentos, descomente para rodar o que desejar.
    # Por padrão soma todos os BUs previamente baixados.

    sum_all_bus()
