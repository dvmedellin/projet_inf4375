#!/usr/bin/python3
# -*- coding: utf-8 -*-
# =============================================================================
# Projet de session INF4375-20, E15, UQAM
# Fichier   : csvtodb.py
# Auteur    : Carlos Nossa (NOSC05018807)
# =============================================================================

'''Description : Transforme les fichiers csv en format json. Puis les donnees sont inserees dans une bade de donnees ElasticSearch.'''

# ====================================================================
# Library importation
# --------------------------------------------------------------------
import json
import io
import sys
import os
import string
from elasticsearch import Elasticsearch
from jsonschema import validate
from urllib.request import urlopen

# ====================================================================
# Constants definition
# --------------------------------------------------------------------
SEP = ','
COL_NAMES = ["FOURNISSEUR","NO DE DOSSIER","DIRECTION","SERVICE","DESCRIPTION","ACTIVITÉ","NO DÉCISION","APPROBATEUR","DATE","MONTANT","RÉPARTITION"]
SOURCE = 'http://donnees.ville.montreal.qc.ca/dataset'
CSV_FILES = ['74efbfc7-b1bd-488f-be6f-ad122f1ebe8d/resource/a7c221f7-7472-4b01-9783-ed9e847ee8c1/download/contratsfonctionnaires.csv', \
                      '505f2f9e-8cec-43f9-a83a-465717ef73a5/resource/87a6e535-3a6e-4964-91f5-836cd31099f7/download/contratscomiteexecutif.csv', \
                      '6df93670-af44-492e-a644-72643bf58bc0/resource/35e636c1-9f99-4adf-8898-67c2ea4f8c47/download/contratsconseilagglomeration.csv', \
                      '6df93670-af44-492e-a644-72643bf58bc0/resource/a6869244-1a4d-4080-9577-b73e09d95ed5/download/contratsconseilmunicipal.csv']

# ====================================================================
# Main processing
# --------------------------------------------------------------------

def main():
    print('Starting...')
   
    es = Elasticsearch([{'host':'localhost','port':'9200'}])
    create_index(es,'contracts')   
   
    for file in CSV_FILES :
        print('reading file {}'.format(file))
        rows_dict = readcsv(file)
        json_file = tojson(rows_dict)
        insert_data(es,'contracts', json_file)
    print('Finished')
    return(0) 
#end of main

def readcsv(file):
    rows_dict = []
    filename = '{}/{}'.format(SOURCE,file)
    try:
        f=io.StringIO(urlopen(filename).read().decode('utf-8'), newline='\n')      
    except :
        print('WARNING : Cannot open file {}. Ignoring file'.format(filename))
        return []
   
    hdr = f.readline().strip()
    while hdr[0]=='#': hdr = f.readline().strip()
   
    hdr=hdr.replace('\n', '').replace('\r','').replace('\t', '')
    hdr=hdr.replace("DATE D'APPROBATION","DATE").replace("DATE SIGNATURE","DATE").replace("NOM DU FOURNISSEUR","FOURNISSEUR").replace("NUMÉRO","NO DE DOSSIER").replace("OBJET","DESCRIPTION")
    hdr=hdr.replace("PORTION À LA CHARGE DE L'AGGLO.","RÉPARTITION")
    colname = hdr.split(SEP)
    colname = [item.strip('"') for item in colname]
   
    missing_cols = []
    for item in COL_NAMES:
        if colname.count(item) == 0: missing_cols.append(item) 
   
    tmp = ''
    line = f.readline().strip()
    while line: 
        tmp += line.replace('\n','').replace('\r','').replace('\t','')

        if tmp.count('"')%2 != 0 or tmp.count(SEP)<len(colname)-1:
            line = f.readline().strip()
            continue
      
        tmplist = tmp.split(SEP) 
        if len(tmplist) != len(colname) :
            row = [] 
            n = 0
            while n < len(tmplist) :
                e = tmplist[n].strip()
                if len(e)>0:
                    while e.count('"')%2!=0 or (e[0]=='"' and e[-1]!='"') :
                        if n < len(tmplist)-1:
                            n+=1 
                            e= '{}{} {}'.format(e,SEP,tmplist[n].strip())
                row.append(e.strip('"'))
                n+=1
        else : 
            row = tmplist
      
        col_dict = {}
        for n in range(0, len(colname)): col_dict[colname[n].strip()] = str(row[n]).strip()      
        for col in missing_cols: col_dict[col]=''
      
        rows_dict.append(col_dict)   
        tmp = ''
        line = f.readline().strip()
   
    return rows_dict

#
def tojson(row_dict):   
    json_bufr = io.StringIO(newline='\n')
    try:         
        json.dump(row_dict,json_bufr, ensure_ascii=False)
    except:
        print('WARNING : Could not put information to a json format. File ignored')   
    return json_bufr

#insert information into elasticsearch
def insert_data(es,name,json_file):
    properties = {}
    for k in COL_NAMES: 
        properties[k]= {'type':'string'}
    json_schema = {'type':'object', 'properties': properties}
    contract_list = json.loads(json_file.getvalue())
    for contract in contract_list:
        try:
            validate(contract, json_schema)
            es.index(index='contracts', body=contract,  doc_type='contract')
        except:
            print('WARNING : Format not valid. Ignoring contract {}'.format(contract))

#create index in Elasticsearh server. if it already exists destroy it before recreating it
def create_index(es,name) :
    if es.indices.exists(name) : es.indices.delete(index = name)   
    
    request_body = {'settings' : {'number_of_shards': 1}, "mappings" : {'_default_' : {'date_detection' : 0}}}
    es.indices.create(index = name,  body=request_body)

if __name__ == "__main__":
   main()
