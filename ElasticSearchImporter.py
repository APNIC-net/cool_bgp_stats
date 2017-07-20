# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 13:46:12 2017

@author: sofiasilva
"""
import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
import sys, getopt
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch import exceptions

import delStats_ES_properties
import prefStats_ES_properties
import ASesStats_ES_properties
import updatesStats_ES_properties
import deaggStats_ES_properties

class ElasticSearchImporter:
    
    def __init__(self, host):
        config = {
             'host': host
             }
        self.ES = Elasticsearch([config,], timeout=300)
    	    
    def createIndex(self, mapping_dict, index_name):
        if not self.ES.indices.exists(index = index_name):        
            request_body = {
                    	    "settings" : {
                                 "number_of_shards": 5,
                                 "number_of_replicas": 1
                                 },                
                            "mappings": mapping_dict
                            }
                            
            print("creating {} index...".format(index_name))
            self.ES.indices.create(index = index_name, body = request_body, ignore=400)
            self.ES.indices.refresh(index = index_name)
    
    def prepareData(self, data_for_es, index_name, doc_type, unique_index):
        bulk_data = []
                                                
        for index, row in data_for_es.iterrows():
            data_dict = row.to_dict()
            op_dict = {
                    "_index": index_name,
                    "_type": doc_type,
                    "_source": data_dict
                        }  

            bulk_data.append(op_dict)
    
        return bulk_data
         
    def inputData(self, index_name, bulk_data, numOfDocs):
        helpers.bulk(self.ES, bulk_data)
        self.ES.indices.refresh(index_name)
        
        # check data is in there, and structure in there
        try:
            self.ES.indices.get_mapping(index = index_name)
            if self.ES.count(index_name)['count'] == numOfDocs:
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
    DELEGATED = False
            
    try:
        opts, args = getopt.getopt(argv, "hDPAp:H:", ["files_path=", "host="])
    except getopt.GetoptError:
        print 'Usage: ElasticSearchImporter.py -h | ( -D | -P | -A | -d | -U) -p <files path> -H <Elasticsearch host>'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script stores into ElasticSearch the statistics contained in the JSON files in the provided folder."
            print 'Usage: ElasticSearchImporter.py -h | ( -D | -P | -A | -d | -U) -p <files path> -H <Elasticsearch host>'
            print 'h = Help'
            print 'D = Delegated. Stats about delegations.'
            print 'P = Prefixes. Stats about usage of delegated prefixes.'
            print 'A = ASNs. Stats about usage of Autonomous System Numbers.'
            print 'd = Deaggregation. Stats about per prefix deaggregation in the routing table.'
            print 'U = Updates. Stats about per prefix daily BGP updates.'
            print "p = Path to folder containing JSON files. (MANDATORY)"
            print "H = Host running Elasticsearch. (MANDATORY)"
#            print "u = User to save stats to ElasticSearch. (MANDATORY)"
#            print "P = Password to save to stats to ElasticSearch. (MANDATORY)"
            sys.exit()
        elif opt == '-D':
            DELEGATED = True
            ES_properties = delStats_ES_properties
        elif opt == '-P':
            ES_properties = prefStats_ES_properties
        elif opt == '-A':
            ES_properties = ASesStats_ES_properties
        elif opt == '-d':
            ES_properties = deaggStats_ES_properties
        elif opt == '-U':
            ES_properties = updatesStats_ES_properties
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
        esImporter = ElasticSearchImporter(host)
        esImporter.createIndex(ES_properties.mapping,
                               ES_properties.index_name)
        numOfDocs = esImporter.ES.count(ES_properties.index_name)['count']
                
        for json_file in os.listdir(files_path):
            if json_file.endswith(".json"):
                plain_df = pd.read_json('%s/%s' % (files_path, json_file), orient = 'index').reset_index()
                if len(plain_df) > 0:
                    if DELEGATED:
                        plain_df['GeographicArea'] = plain_df['Geographic Area']
                        del plain_df['Geographic Area']
                        
                    plain_df = plain_df.fillna(-1)
    
                    bulk_data, numOfDocs = esImporter.prepareData(plain_df,
                                                                  ES_properties.index_name,
                                                                  ES_properties.doc_type,
                                                                  numOfDocs,
                                                                  ES_properties.unique_index)
                                                        
                    dataImported = esImporter.inputData(ES_properties.index_name,
                                                        bulk_data, numOfDocs)
    
                    if dataImported:
                        sys.stderr.write("Stats from file %s saved to ElasticSearch successfully!\n" % json_file)
                    else:
                        sys.stderr.write("Stats from file %s could not be saved to ElasticSearch.\n" % json_file)

                else:
                    sys.stderr.write("Stats file %s is empty.\n" % json_file)

    else:
        print "A host in which ElasticSearch is running MUST be provided."
        sys.exit()

        
if __name__ == "__main__":
    main(sys.argv[1:])