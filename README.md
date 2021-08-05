# Totalizar do segundo turno das eleições de 2014 (Aécio e Dilma)

O script totalizador.py é capaz de:

- Fazer o download de todos os BUs do site do TSE
- Extrair os dados de BUs, inseri-los em banco de dados PostgreSQL, e totalizá-los

No meu PC (relativamente parrudo) baixar os BUs demora cerca de 3 horas,
e extrair/totalizar os dados demora 15 minutos.

Os BUs ocupam 15 GB em disco, 3,6 GB zipados. (Vou tentar achar um lugar para
armazená-los...).

## Quero analisar os dados extraídos

Descomprima e carregue o dump PostgreSQL (`db.sql.zip`). Ele criará uma tabela chamada
`presidente_2014_2`. Com ela você pode facilmente calcular os totais, por exemplo:

```sql
select sum(aecio) from presidente_2014_2 ;
   sum
----------
 51041155
(1 row)
```

```sql
select sum(dilma) from presidente_2014_2 ;
   sum
----------
 54501118
(1 row)
```

## Quero ver os BUs

Enquanto não acho um lugar para hospedar 3,6 GB de dados, você precisará baixá-los.
Veja o arquivo totalizador.py.

## Dependências

Dependências Python estão em `requirements.txt`.

Também é preciso baixar o `chromedriver` do Chrome e colocá-lo na pasta raiz do projeto.
