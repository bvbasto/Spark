# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC o ficheiros /training/filesDub/data.csv tem uma lista dos ficheiros existentes num determinado diretorio
# MAGIC o ficheiros /training/filesDub/gest.csv tem uma lista das repetiçõs desses ficheiros. Cada repetição passa a ter um sufixo que é indicado na própria tabela 
# MAGIC 
# MAGIC crie um novo ficheiro em que cada linha tem o nome do ficheiro principal e o numero de repetições que exitem em gest e se existem linhas no ficheiro de repetições que não têm referência no ficheiro data