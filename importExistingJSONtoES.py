# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 13:46:12 2017

@author: sofiasilva
"""

import sys, getopt, os
import hashlib
import pandas as pd
import elasticsearch

def connect(user, password):
    # configure elasticsearch
	config = {
#	    'host': 'twerp.matong.apnic.net'
         'host': 'localhost',
         'user': user,
         'password': password
	}
	return elasticsearch.Elasticsearch([config,], timeout=300)

def createIndex(es, mapping_dict, index_name):
    request_body = {
	    "settings" : {
	        "number_of_shards": 5,
	        "number_of_replicas": 1
	    },

	    'mappings': mapping_dict
	}

    print("creating {} index...".format(index_name))
    es.indices.create(index = index_name, body = request_body)

def prepareData(data_for_es, index_name, index_type):
    bulk_data = []

    for index, row in data_for_es.iterrows():
        data_dict = {}
        for i in range(len(row)):
            data_dict[data_for_es.columns[i]] = row[i]

        op_dict = {
            "index": {
                "_index": index_name,
                "_type": index_type,
                "_id": '{}_{}'.format(data_dict['_id'], data_dict['Date'])
                    }
                    }
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)
    
    return bulk_data
     
def inputData(es, index_name, bulk_data):
    es.bulk(index = index_name, body = bulk_data)

    # check data is in there, and structure in there
    es.search(body={"query": {"match_all": {}}}, index = index_name)
    es.indices.get_mapping(index = index_name)
 

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
    plain_df['_id'] = plain_df['Date'].astype('str') + '_' + plain_df['index'].astype('str')
    del plain_df['index']
    del plain_df['multiindex_comb']
    
def main(argv):    

    files_path = ''
    user = ''
    password = ''
    
    files_path = '/Users/sofiasilva/BGP_files'
    user = 'elastic'
    password = 'q1w2e3r4'
    
    # Mapping for stats about delegations
    delStats_mapping = {"_default_" : {
                            "properties" : {
                                "stat_id" : {
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
        opts, args = getopt.getopt(argv, "hp:u:P:", ["files_path=", "user=", "password="])
    except getopt.GetoptError:
        print 'Usage: importExistingJSONtoES.py -h | -p <files path -u <ElasticSearch user> -P <ElasticSearch password>'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script stores into ElasticSearch the statistics contained in the JSON files in the provided folder."
            print 'Usage: importExistingJSONtoES.py -h | -p <files path -u <ElasticSearch user> -P <ElasticSearch password>'
            print 'h = Help'
            print "p = Path to folder containing JSON files. (MANDATORY)"
            print "u = User to save stats to ElasticSearch."
            print "P = Password to save to stats to ElasticSearch."
            sys.exit()
        elif opt == '-p':
            files_path = arg
        elif opt == '-u':
            user = arg
        elif opt == '-P':
            password = arg
        else:
            assert False, 'Unhandled option'

    if user != '' and password != '':
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
                
                es = connect(user, password)
                createIndex(es, delStats_mapping, del_stats_index_name)
                preparePlainDF(plain_df)
                bulk_data = prepareData(plain_df, del_stats_index_name,\
                                        del_stats_index_type)
                inputData(es, del_stats_index_name, bulk_data)
                
                # TODO Check if success
                sys.stderr.write("Stats from file %s saved to ElasticSearch successfully!\n" % json_file)

    else:
        print "User and Password for ElasticSearch MUST be provided."
        sys.exit()

        
if __name__ == "__main__":
    main(sys.argv[1:])