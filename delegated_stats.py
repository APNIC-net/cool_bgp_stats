import sys
#import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
import datetime

DEBUG = True

if DEBUG:
    today = '20170123'
else:
    today = datetime.date.today().strftime('%Y%m%d')


bgp_path = '/Users/sofiasilva/GitHub/cool_bgp_stats'

sys.path.append(bgp_path)
from get_file import get_file

dest_file_delegated = '%s/BGP_files/delegated_apnic_%s.txt' % (bgp_path, today)
delegated_url = 'https://ftp.apnic.net/stats/apnic/delegated-apnic-latest'

if not DEBUG:
    get_file(delegated_url, dest_file_delegated)


delegated_df = pd.read_csv(
                dest_file_delegated,
                sep = '|',
                header=None,
                names= [
                    'registry',
                    'cc',
                    'resource_type',
                    'initial_resource',
                    'count',
                    'date',
                    'alloc_assig'
                ],
                index_col=False,
#                parse_dates=['date'],
#                infer_datetime_format=True,
                comment='#'
            )
            

if DEBUG:
   res_types = ['All', 'asn', 'ipv4']
   del_types = ['All', 'allocated', 'assigned']
   granularities = ['All', 'annually'] 

   delegated_df = pd.concat([delegated_df[10:90], delegated_df[10000:10080]])
   
   initial_date = datetime.datetime.strptime(str(min(delegated_df['date'])), '%Y%m%d')
   final_date = datetime.datetime.strptime(str(max(delegated_df['date'])), '%Y%m%d')
   
   
else:
    res_types = ['All', 'asn', 'ipv4', 'ipv6']
    del_types = ['All', 'allocated', 'assigned']
    granularities = ['All', 'daily', 'weekly', 'monthly', 'annually']

    initial_date = datetime.datetime.strptime(str(delegated_df.loc[0, 'count']), '%Y%m%d')
    final_date = datetime.datetime.strptime(str(delegated_df.loc[0, 'date']), '%Y%m%d')

    summary_records = pd.DataFrame()

    for i in range(4):
        summary_records.at[i, 'Type'] = res_types[i]

        if i == 0:
            summary_records.at[i, 'count'] = int(delegated_df.loc[i, 'initial_resource'])
        else:
            summary_records.at[i, 'count'] = int(delegated_df.loc[i, 'count'])


    total_rows = delegated_df.shape[0]
    delegated_df = delegated_df[int(total_rows-summary_records.loc[0, 'count']):total_rows]

delegated_df['date'] = pd.to_datetime(delegated_df['date'], format='%Y%m%d')


delegated_df.ix[pd.isnull(delegated_df.cc), 'cc'] = 'XX'
CCs = ['All']
CCs.extend(set(delegated_df['cc'].values))


def countries_loop(res_df, g, r, d, stats_df):
  for c in CCs:
    if c == 'All':
        country_res_df = res_df
    else:
        country_res_df = res_df[res_df['cc'] == c]

    if g == 'All':
        stats_df.loc[c, r, d, 'All', 'All'] = len(country_res_df)
    else:
        if not country_res_df.empty:
            if g == 'annually':
                date_groups = country_res_df.groupby(country_res_df['date'].map(lambda x: x.year))
            elif g == 'monthly':
                date_groups = country_res_df.groupby(country_res_df['date'].map(lambda x: x.strftime('%Y%m')))
            elif g == 'weekly':
                date_groups = country_res_df.groupby(country_res_df['date'].map(lambda x: x.strftime('%Y%W')))
            else:
                date_groups = country_res_df.groupby(country_res_df['date'].map(lambda x: x.strftime('%Y%m%d')))

            counts = date_groups.size()
                
            for date in dates_dic[g]:
                if int(date) in counts.index: 
                    stats_df.loc[c, r, d, g, date] = counts[int(date)]
                else:
                    stats_df.loc[c, r, d, g, date] = 0
                
  return stats_df


# Just to verify
if not DEBUG:
    num_rows = len(delegated_df)
    if not num_rows == summary_records[summary_records['Type'] == 'All']['count']:
      print 'THERE\'S SOMETHING WRONG!'
    
    for r in res_types[1:4]:
      total = len(delegated_df[delegated_df['resource_type'] == r])
      if not total == summary_records[summary_records['Type'] == r]['count']:
        print 'THERE\'S SOMETHING WRONG WITH THE NUMBER OF %s' % r

# Create empy Data Frame
stats_df = pd.DataFrame()

dates_dic = dict()

#Fill it with -1 for all the possible combinations of column values
for g in granularities:
    if g == 'daily':
        dates = pd.date_range(start=initial_date, end=final_date, freq='D')
        dates = dates.strftime('%Y%m%d')
    elif g == 'weekly':
        dates = pd.date_range(start=initial_date, end=final_date, freq='W')
        dates = dates.strftime('%Y%W')
    elif g == 'monthly':
        dates = pd.date_range(start=initial_date, end=final_date, freq='M')
        dates = dates.strftime('%Y%m')
    elif g == 'annually':
        dates = pd.date_range(start=initial_date, end=final_date, freq='A')
        dates = dates.strftime('%Y')
    else:
        dates = ['All']

    #Save valid dates for current granularity    
    dates_dic[g] = dates


    iterables = [CCs, res_types, del_types, granularities, dates]
    index = pd.MultiIndex.from_product(iterables,
                                       names=[
                                           'Country',
                                           'ResourceType',
                                           'DelegationType',
                                           'Granularity',
                                           'Date'
                                        ]
                                       )
    aux_df = pd.DataFrame(index=index, columns=[
                                                   'NumOfDelegations'
                                                   ]
                            )
    aux_df = aux_df.fillna(-1)
    stats_df = pd.concat([stats_df, aux_df])
    stats_df.sort_index()

for g in granularities:
  for r in res_types:
    if r == 'All':
      res_df = delegated_df
    else:
      res_df = delegated_df[delegated_df['resource_type'] == r]
    
    if r == 'ipv4' or r == 'ipv6':
      for d in del_types:
        if d == 'All':
          del_res_df = res_df
        else:
          del_res_df = res_df[res_df['alloc_assig'] == d]
        
        stats_df = countries_loop(del_res_df, g, r, d, stats_df)
      
    else:
      stats_df = countries_loop(res_df, g, r, 'All', stats_df)  

# TODO Verificar tuplas del index de stats_df que no tienen sentido
# TODO Verificar campos de stats_df que no se completan


#write.csv2(stats_df, file=paste(c(bgp_path, '/BGP_files/delegated_stats.csv'), sep='', collapse = ''), row.names = T)
#
#
#JSONstat <- toJSONstat(stats_df)
#write(JSONstat, file=paste(c(bgp_path, "/BGP_files/delegated_stats.json"), collapse=''))



#TODO luego de tener las estadisticas hasta el momento, tengo que escribir codigo
# que solo lea lo del ultimo mes y lo agregue al data frame de estadisticas
# y luego genere el nuevo JSON
# actualizar las estadisticas existentes que cambien ('All')
