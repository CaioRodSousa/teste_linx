#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import io
import pytz
import datetime as DT
import zipfile
import requests
import urllib.request
import boto3


# In[2]:


#Gera a data e hora de São Paulo Brasil
tz_BR = pytz.timezone('America/Sao_Paulo')
#Gera a data atual em formato Timestamp
start = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
print("Processo iniciado em: "+start)


# In[10]:


# Configuração do cliente S3
s3 = boto3.client('s3')

#configuração do cliente S3
s3_resource = boto3.resource('s3') 

# Configuração da URL base para download dos arquivos
base_url = 'http://200.152.38.155/CNPJ/'

# Nome do bucket S3
bucket_name = 'linx-bucket'

# Nome da pasta dentro do bucket para armazenar os arquivos
folder_name = 'receita_federal/'

# Nome da pasta para armazenar os arquivos descompactados
path_raw = f'{folder_name}raw/'

# Define os paths de cada pasta destino
path_empresas = f'{folder_name}empresas/'

path_estabelecimentos = f'{folder_name}estabelecimentos/'

path_simples = f'{folder_name}simples/'

path_paises = f'{folder_name}paises/'

path_municipios = f'{folder_name}municipios/'

# Nome dos arquivos para download
files =      ['Empresas0.zip',
              'Empresas1.zip',
              'Empresas2.zip',
              'Empresas3.zip',
              'Empresas4.zip',
              'Empresas5.zip',
              'Empresas6.zip',
              'Empresas7.zip',
              'Empresas8.zip',
              'Empresas9.zip',
              'Estabelecimentos0.zip',
              'Estabelecimentos1.zip',
              'Estabelecimentos2.zip',
              'Estabelecimentos3.zip',
              'Estabelecimentos4.zip',
              'Estabelecimentos5.zip',
              'Estabelecimentos6.zip',
              'Estabelecimentos7.zip',
              'Estabelecimentos8.zip',
              'Estabelecimentos9.zip',
              'Simples.zip',
              'Paises.zip',
              'Municipios.zip']


# In[4]:


# Verifica se o bucket existe
if s3.list_buckets()['Buckets']:
    for bucket in s3.list_buckets()['Buckets']:
        if bucket_name == bucket['Name']:
            end   = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
            print(f'O bucket {bucket_name} já existe!: {end}')
            break
else:
    # Cria o bucket
    s3.create_bucket(Bucket=bucket_name)
    end   = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
    print(f'O bucket {bucket_name} foi criado com sucesso!: {end}')


# In[5]:


# Verifica se a pasta existe no bucket
folder_exists = False
result = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
if 'Contents' in result:
    folder_exists = True

if folder_exists:
    # Exclui todo o conteúdo da pasta
    objects_to_delete = [{'Key': obj['Key']} for obj in result['Contents']]
    s3.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
    s3.put_object(Bucket=bucket_name, Key=folder_name, Body='')
    end   = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
    print(f'O conteúdo da pasta {folder_name} ja existe e foi substituido com sucesso!: {end}')
else:
    # Cria a pasta
    s3.put_object(Bucket=bucket_name, Key=folder_name, Body='')
    end   = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
    print(f'A pasta {folder_name}foi criada com sucesso!: {end}')


# In[7]:


# cria a pasta "receita_federal/raw"
s3.put_object(Bucket=bucket_name, Key=path_raw)
end   = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
print(f'A pasta {path_raw} foi criada com sucesso!: {end}')


# In[8]:


# Define a função para fazer o download dos arquivos
def download_file(url, local_filename):
    # Faz o download do arquivo em chunks para evitar problemas de memória
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


# Faz o download dos arquivos e envia para o S3 na pasta raw
end   = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
print(f'dowload dos arquivos zip foi iniciado: {end}')
for file in files:
    url = f'{base_url}{file}'
    local_file = f'{file}'
    while True:
        try:
            # Faz o download do arquivo
            download_file(url, local_file)
            # Envia o arquivo para o S3 na pasta raw
            s3.upload_file(local_file, bucket_name, path_raw + file)
            # Remove o arquivo local
            os.remove(local_file)
            end   = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
            print(f'O arquivo {file} foi baixado e enviado com sucesso!: {end}')
            break
        except Exception as e:
            end   = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
            print(f'Erro: {e}\nTentando novamente o download do arquivo {file}...: {end}') 


# In[11]:


end   = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
print(f'processo de descompactação iniciado: {end}')

for obj in s3_resource.Bucket(bucket_name).objects.filter(Prefix=path_raw):
    # Verifica se o objeto é um arquivo zip
    if obj.key.endswith('.zip'):
        # Determina o destino com base no nome do arquivo zip
        if 'Empresas' in obj.key:
            destination = path_empresas
        elif 'Estabelecimentos' in obj.key:
            destination = path_estabelecimentos
        elif 'Simples' in obj.key:
            destination = path_simples
        elif 'Paises' in obj.key:
            destination = path_paises
        elif 'Municipios' in obj.key:
            destination = path_municipios
        
        # Extrai os arquivos do zip
        zip_obj = s3_resource.Object(bucket_name, obj.key)
        buffer = zip_obj.get()['Body'].read()
        zip_file = zipfile.ZipFile(io.BytesIO(buffer))
        for file in zip_file.namelist():
            # Salva cada arquivo em seu destino apropriado
            s3_resource.meta.client.upload_fileobj(
                zip_file.open(file),
                Bucket=bucket_name,
                Key=destination + file
            )
        end = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
        print(f'Arquivo {file} foi descompactado para {destination}: {end}')

end = DT.datetime.strftime(DT.datetime.now(tz_BR),'%Y-%m-%d %H:%M:%S')
print(f'processo finalizado!: {end}')                  


# In[ ]:




