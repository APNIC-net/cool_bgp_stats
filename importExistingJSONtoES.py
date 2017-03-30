# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 13:46:12 2017

@author: sofiasilva
"""

import sys, getopt, os
import hashlib
import pandas as pd
import elasticsearch

def connect(host):
    # configure elasticsearch
	config = {
         'host': host
	}
	return elasticsearch.Elasticsearch([config,], timeout=300)

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

def prepareData(data_for_es, index_name, index_type, id_column):
    bulk_data = []

    for index, row in data_for_es.iterrows():
        data_dict = {}
        for i in range(len(row)):
            data_dict[data_for_es.columns[i]] = row[i]

        op_dict = {
            "index": {
                "_index": index_name,
                "_type": index_type,
                "_id": data_dict[id_column]
                    }
                    }
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)
    
    return bulk_data, len(data_for_es)
     
def inputData(es, index_name, bulk_data, numOfDocs):
    es.bulk(index = index_name, body = bulk_data, refresh = True)

    # check data is in there, and structure in there
    try:
        res = es.search(body={"query": {"match_all": {}}}, index = index_name)
        es.indices.get_mapping(index = index_name)
        if len(res['hits']['hits']) == numOfDocs:
            return True
        else:
            return False
    except elasticsearch.exceptions.NotFoundError:
        return False
 

def hashFromColValue(col_value):
    return hashlib.md5(col_value).hexdigest()
    
def preparePlainDF(plain_df):
    plain_df['GeographicArea'] = plain_df['Geographic Area']
    del plain_df['Geographic Area']
    
    plain_df['multiindex_comb'] = plain_df['GeographicArea'] +\
                                    plain_df['ResourceType'] +\
                                    plain_df['Status'] +\
                                    plain_df['Organization']
                                    
    plain_df['index'] = plain_df['multiindex_comb'].apply(hashFromColValue)
    plain_df['del_stat_id'] = plain_df['Date'].astype('str') + '_' + plain_df['index'].astype('str')
    del plain_df['index']
    del plain_df['multiindex_comb']
    
def main(argv):    

    files_path = ''
    host = ''
#    user = ''
#    password = ''
    
    id_column = 'del_stat_id'
    
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
                                    "type" : "integer"
                                            },
                                "IPSpace" : {
                                    "type" : "integer"
                                            }
                                        }
                                    }
                                }
    del_stats_index_name = 'delegated_stats'
    del_stats_index_type = 'id'
    
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
        for json_file in os.listdir(files_path):
            if json_file.endswith(".json"):
                plain_df = pd.read_json('%s/%s' % (files_path, json_file), orient = 'index').reset_index()
                del plain_df['index']
                # Originally stats were oomputed considering Org = All,
                # ResourceType = All, Status = All, GeographicArea = All
                # However, this is not necessary as it contains redundant info
                # and makes it harder to filter the stats when analyzing
                # or visualizing them
                # So we remove these rows of the Data Frame
                plain_df = plain_df[plain_df['Organization'] != 'All']
                plain_df = plain_df[plain_df['Geographic Area'] != 'All']
                plain_df = plain_df[plain_df['ResourceType'] != 'All']
                plain_df = plain_df[plain_df['Status'] != 'All']
                
                preparePlainDF(plain_df)
                bulk_data, numOfDocs = prepareData(plain_df, del_stats_index_name,\
                                                    del_stats_index_type, id_column)
                dataImported = inputData(es, del_stats_index_name, bulk_data, numOfDocs)

                if dataImported:
                    sys.stderr.write("Stats from file %s saved to ElasticSearch successfully!\n" % json_file)
                else:
                    sys.stderr.write("Stats from file %s could not be saved to ElasticSearch.\n" % json_file)

    else:
        print "A host in which ElasticSearch is running MUST be provided."
        sys.exit()

        
if __name__ == "__main__":
    main(sys.argv[1:])