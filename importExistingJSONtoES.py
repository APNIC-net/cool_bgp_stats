# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 13:46:12 2017

@author: sofiasilva
"""

import sys, getopt, os
import hashlib, json, requests, getpass
import pandas as pd

def hashFromColValue(col_value):
    return hashlib.md5(col_value).hexdigest()
    
def saveToElasticSearch(plain_df, user, password):
    es_host = 'localhost'
    index_name = 'delegated_stats'
    index_type = 'id'
    
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
    df_as_json = plain_df.to_json(orient='records', lines=True)

    final_json_string = ''
    for json_document in df_as_json.split('\n'):
        jdict = json.loads(json_document)
        # Header line
        metadata = json.dumps({'index': {'_index': index_name,\
                                        '_type': index_type,\
                                        '_id': jdict['_id']}})
        jdict.pop('_id')
        final_json_string += metadata + '\n' + json.dumps(jdict) + '\n'
    
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post('http://%s:9200/%s/%s/_bulk' % (es_host, index_name, index_type), data=final_json_string, headers=headers, timeout=60) 
    
    if (r.status_code == 401):
        if user == '' and password == '':
            print("Authentication needed. Please enter your username and password")
            user = raw_input("Username: ")
            password = getpass.getpass("Password: ")

        r = requests.post('http://%s:9200/%s/%s/_bulk' %\
                            (es_host, index_name, index_type),\
                            data=final_json_string, headers=headers,\
                            timeout=60, auth=(user, password)) 
    
    return r

def main(argv):    

    files_path = ''
    user = ''
    password = ''
    
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

                r = saveToElasticSearch(plain_df, user, password)
                status_code = r.status_code
                if status_code == 200:
                    sys.stderr.write("Stats from file %s saved to ElasticSearch successfully!\n" % json_file)
                else:
                    print "Something went wrong when trying to save stats from file %s to ElasticSearch.\n" % json_file
    else:
        print "User and Password for ElasticSearch MUST be provided."
        sys.exit()

        
if __name__ == "__main__":
    main(sys.argv[1:])