#from django.shortcuts import render
from django.http import HttpResponse
import pandas as pd
import json

files_path = '/Users/sofiasilva/BGP_files'

def index(request):
    return HttpResponse("Hello, world. You're at the getOrgStats index.")
    
# TODO Allow empty year (all years available)
# TODO Allow range of dates
# TODO Accept Geographic Area, Resource Type, Status, Granularity
# TODO As Greographic Area accept All, All Countries and All Regions
# TODO Add to DataFrame routing stats
def getJSON(request, opaque_id, year):
    #TODO List files_path folder and find file corresponding to year
    json_file = '%s/delegated_stats_%s_20170201.json' % (files_path, year)
    stats_df = pd.read_json(json_file, orient = 'index')
    org_df = stats_df[stats_df['Organization'] == opaque_id]
    if org_df.shape[0] > 0:
        org_json = org_df.to_json(orient='records')
    else:
        org_json = {}

    response = json.dumps(json.loads(org_json), sort_keys=True, indent=4) 
    return HttpResponse(response, content_type='application/json')

