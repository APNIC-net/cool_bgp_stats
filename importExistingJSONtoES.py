# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 13:46:12 2017

@author: sofiasilva
"""

import sys, getopt, os
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch import exceptions

def connect(host):
    # configure elasticsearch
	config = {
         'host': host
	}
	return Elasticsearch([config,], timeout=300)

def createIndex(es, mapping_dict, index_name):
    request_body = {
            	    "settings" : {
                         "number_of_shards": 5,
                         "number_of_replicas": 1
                         },                
                    "mappings": mapping_dict
                    }
                    
    print("creating {} index...".format(index_name))
    es.indices.create(index = index_name, body = request_body, ignore=400)
    es.indices.refresh(index = index_name)

def prepareData(es, data_for_es, index_name, doc_type, numOfDocs):
    bulk_data = []
    
    for index, row in data_for_es.iterrows():
        data_dict = {}
        
        for i in range(len(row)):
            data_dict[data_for_es.columns[i]] = row[i]

        if len(res = es.search(index=index_name, doc_type=doc_type,
                               body={'query':{
                                       'bool':{
                                           'must':[{
                                               'match':{
                                                   'ResourceType':data_dict['ResourceType']
                                                       }}, {
                                                'match':{
                                                    'Date':data_dict['Date']
                                                        }}, {
                                                'match':{
                                                    'Organization':data_dict['Organization']
                                                        }}, {
                                                'match':{
                                                    'GeographicArea':data_dict['GeographicArea']
                                                        }}, {
                                                'match':{
                                                    'Status':data_dict['Status']
                                                        }}]}}})) == 0:

            op_dict = {
                    "_op_type": "index",
                    "_index": index_name,
                    "_type": doc_type,
                    "_id": numOfDocs,
                    "_source": data_dict
                        }  
    
            numOfDocs += 1

            bulk_data.append(op_dict)

    return bulk_data, numOfDocs
     
def inputData(es, index_name, bulk_data, numOfDocs):
    helpers.bulk(es, bulk_data)
    es.indices.refresh(index_name)
    
    # check data is in there, and structure in there
    try:
        es.indices.get_mapping(index = index_name)
        if es.count(index_name)['count'] == numOfDocs:
            return True
        else:
            return False
    except exceptions.NotFoundError:
        return False
 
    
def main(argv):    

    files_path = ''
    host = ''
#    user = ''
#    password = ''
        
    # Mapping for stats about delegations
    delStats_mapping = {"_default_" : {
                            "properties" : {
                                "del_stat_id" : {
                                    "type": "integer"
                                            },
                                "GeographicArea" : {
                                    "index" : "analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "ResourceType" : {
                                    "index" : "not_analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "Status" : {
                                    "index" : "not_analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "Organization" : {
                                    "index" : "not_analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "Date" : {
                                    "type": "date",
                                    "format": "yyyyMMdd"
                                        },
                                "NumOfDelegations" : {
                                    "type" : "integer"
                                                    },
                                "NumOfResources" : {
                                    "type" : "integer"
                                                    },
                                "IPCount" : {
                                    "type" : "long"
                                            },
                                "IPSpace" : {
                                    "type" : "long"
                                            }
                                        }
                                    }
                                }
    del_stats_index_name = 'delegated_stats_index'
    del_stats_doc_type = 'delegated_stats'
    
    try:
        opts, args = getopt.getopt(argv, "hp:H:", ["files_path=", "host="])
    except getopt.GetoptError:
        print 'Usage: importExistingJSONtoES.py -h | -p <files path> -H <Elasticsearch host>'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script stores into ElasticSearch the statistics contained in the JSON files in the provided folder."
            print 'Usage: importExistingJSONtoES.py -h | -p <files path> -H <Elasticsearch host>'
            print 'h = Help'
            print "p = Path to folder containing JSON files. (MANDATORY)"
            print "H = Host running Elasticsearch. (MANDATORY)"
#            print "u = User to save stats to ElasticSearch. (MANDATORY)"
#            print "P = Password to save to stats to ElasticSearch. (MANDATORY)"
            sys.exit()
        elif opt == '-p':
            files_path = arg
        elif opt == '-H':
            host = arg
#        elif opt == '-u':
#            user = arg
#        elif opt == '-P':
#            password = arg
        else:
            assert False, 'Unhandled option'

    if host != '':
        es = connect(host)
        createIndex(es, delStats_mapping, del_stats_index_name)
        numOfDocs = es.count(del_stats_index_name)['count']
                
        for json_file in os.listdir(files_path):
            if json_file.endswith(".json"):
                plain_df = pd.read_json('%s/%s' % (files_path, json_file), orient = 'index').reset_index()
                if len(plain_df) > 0:
                    plain_df['GeographicArea'] = plain_df['Geographic Area']
                    del plain_df['Geographic Area']
                    plain_df = plain_df.fillna(-1)
    
                    bulk_data, numOfDocs = prepareData(es, plain_df, del_stats_index_name,
                                                        del_stats_doc_type, numOfDocs)
                                                        
                    dataImported = inputData(es, del_stats_index_name, bulk_data, numOfDocs)
    
                    if dataImported:
                        sys.stderr.write("Stats from file %s saved to ElasticSearch successfully!\n" % json_file)
                    else:
                        sys.stderr.write("Stats from file %s could not be saved to ElasticSearch.\n" % json_file)

                else:
                    sys.stderr.write("Stats file %s empty.\n" % json_file)

    else:
        print "A host in which ElasticSearch is running MUST be provided."
        sys.exit()

        
if __name__ == "__main__":
    main(sys.argv[1:])