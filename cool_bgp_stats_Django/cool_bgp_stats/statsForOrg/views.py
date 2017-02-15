from django.shortcuts import get_object_or_404, render
#from django.http import Http404
from django.http import HttpResponse
#from django.template import loader
from .models import StatFile
import pandas as pd
import json

files_path = '/Users/sofiasilva/BGP_files'

def index(request):
    # TODO Include table with general stats for the region
    # Totals so far, yesterday, etc.
    statFiles_list = StatFile.objects.all()
    del_statFiles_list = statFiles_list.filter(stat_type='delegated')
    bgp_statFiles_list = statFiles_list.filter(stat_type='routing')
    context = {
        'del_statFiles_list': del_statFiles_list,
        'bgp_statFiles_list': bgp_statFiles_list
    }  
    
    return render(request, 'statsForOrg/index.html', context)

def getJSONfromDF(df):
    if df.shape[0] > 0:
        df_json = df.to_json(orient='records')
    else:
        df_json = {}

    response = json.dumps(json.loads(df_json), sort_keys=True, indent=4) 
    return HttpResponse(response, content_type='application/json')
    
# TODO Allow empty year (all years available)
# TODO Allow range of dates
# TODO Accept Geographic Area, Resource Type, Status, Granularity
# TODO As Greographic Area accept All, All Countries and All Regions
# TODO Add to DataFrame routing stats
def getJSONforOrgAndYear(request, opaque_id, year):
    json_file = get_object_or_404(StatFile, year=year, stat_type='delegated')
    json_file_path = json_file.output_file
    stats_df = pd.read_json(json_file_path, orient = 'index')
    org_df = stats_df[stats_df['Organization'] == opaque_id]
    return getJSONfromDF(org_df)
    
def getJSONforYear(request, year):
    json_file = get_object_or_404(StatFile, year=year, stat_type='delegated')
    json_file_path = json_file.output_file
    stats_df = pd.read_json(json_file_path, orient = 'index')
    return getJSONfromDF(stats_df)