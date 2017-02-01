#! /usr/bin/python2.7 
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 30 15:45:07 2017

@author: sofiasilva
"""
import sys, getopt
import pandas as pd
import plotly.plotly as py
import plotly.graph_objs as go
import plotly.offline as offline
import matplotlib.pyplot as plt
import matplotlib.dates as dates


offline.init_notebook_mode()
#plotly.tools.set_credentials_file(username='chufia', api_key='??')

from datetime import datetime


def generatePlot(stats_file, country, res_type, status, org, granularity,\
                    initial_date, final_date, stat):
                       
#    stats_df = pd.read_json(stats_file, orient = 'index')
    stats_df = pd.read_csv(stats_file, sep = ',')
    
    stats_df = stats_df[stats_df['Country'] == country]
    
    stats_df = stats_df[stats_df['ResourceType'] == res_type]
    
    stats_df = stats_df[stats_df['Status'] == status]

    stats_df = stats_df[stats_df['Organization'] == org]
        
    if initial_date != '':
        stats_df = stats_df[stats_df['Date'] >= int(initial_date)]

    if final_date != '':
        stats_df = stats_df[stats_df['Date'] <= int(final_date)]
        
    stats_df['Date'] = pd.to_datetime(stats_df['Date'], format='%Y%m%d')
    stats_df.index = stats_df['Date']
        
    if granularity != 'daily':
        if granularity == 'weekly':
            stats_df_r = stats_df.resample('W')
        elif granularity == 'monthly':
            stats_df_r = stats_df.resample('M')
        else: # granularity == 'annually'
            stats_df_r = stats_df.resample('Y')       
        
        stats_df_sum = stats_df_r.sum()
        stats_df_max = stats_df_r.max().to_period()
        stats_df_min = stats_df_r.min().to_period()
        stats_df_mean = stats_df_r.mean().to_period()
        stats_df_std = stats_df_r.std().to_period()

# Static plots

        stats_df_sum[stat].plot()
#   
        fig, ax = plt.subplots()
        ax.plot_date(stats_df_sum.index.to_pydatetime(), stats_df_sum[stat], 'v-')
        ax.xaxis.set_minor_locator(dates.WeekdayLocator(byweekday=(1),
                                                interval=1))
        ax.xaxis.set_minor_formatter(dates.DateFormatter('%d\n%a'))
        ax.xaxis.grid(True, which="minor")
        ax.yaxis.grid()
        ax.xaxis.set_major_locator(dates.MonthLocator())
        ax.xaxis.set_major_formatter(dates.DateFormatter('\n\n\n%b\n%Y'))
        plt.tight_layout()
        plt.show()
#        
        plt.plot_date(stats_df_sum.index.to_pydatetime(), stats_df_sum[stat], fmt='o',\
            tz=None, xdate=True, ydate=False)
#       
        plt.errorbar(stats_df_sum.index, stats_df_mean[stat], stats_df_std[stat], fmt='ok', lw=3)
        plt.errorbar(stats_df_sum.index, stats_df_mean[stat], [stats_df_mean[stat] - stats_df_min[stat],\
                            stats_df_max[stat] - stats_df_mean[stat]], fmt='.k', ecolor='gray', lw=1)
        plt.show()
#        
        fig, ax = plt.subplots()
        ax.plot_date(stats_df_sum.index.to_pydatetime(), stats_df_sum[stat], fmt='g--') # x = array of dates, y = array of numbers        

        fig.autofmt_xdate()

        # For tickmarks and ticklabels every fourth week
        ax.xaxis.set_major_locator(dates.WeekdayLocator(byweekday=dates.MO, interval=4))

        # For tickmarks (no ticklabel) every week
        ax.xaxis.set_minor_locator(dates.WeekdayLocator(byweekday=dates.MO))
        
        ax.xaxis.set_major_formatter(dates.DateFormatter('%Y-%m-%d'))

        # Grid for both major and minor ticks
        plt.grid(True, which='both')
        plt.show()
        
 #       plt.savefig('/Users/sofiasilva/BGP_files/plot.png')
        
    else:
        # Plot.ly for interactive plots
        # https://plot.ly/pandas/time-series/
        data = [go.Scatter(x=stats_df['Date'],\
                y=stats_df[stat])]
    
    layout = dict(
        title='Time series with range slider and selectors',
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                                dict(count=1,
                                     label='1m',
                                     step='month',
                                     stepmode='backward'),
                                dict(count=6,
                                     label='6m',
                                     step='month',
                                     stepmode='backward'),
                                dict(count=1,
                                    label='YTD',
                                    step='year',
                                    stepmode='todate'),
                                dict(count=1,
                                    label='1y',
                                    step='year',
                                    stepmode='backward'),
                                dict(step='all')
                            ])
                        ),
            rangeslider=dict(),
            type='date'
        )
    )  

#   py.iplot(data)
    plotly.offline.plot(data)
#   plotly.offline.iplot(data) 

    plotly.offline.plot(data, image='png')       


    fig = dict(data=data, layout=layout)
    plotly.offline.plot(fig)

    url = plotly.offline.plot(fig, filename='pandas-time-series')
    
    # http://blog.plot.ly/post/117105992082/time-series-graphs-eleven-stunning-ways-you-can
    # For res_type = 'All' or status = 'All' -> Stacked area charts with the
    # values for the different resource types or statuses or stacked bar plot
    # Multiple axis plots to show the different statistics in a single plot
    # Subplots to compare different years, months, etc.
    # Histograms/box plots to show distributions??
    # Daily avg for different months -> bar plots with error bars
    # Heatmaps to show frequency/occurrences of certain combinations between two variables
    
    # Announcement of IPv4 exhaustion?
    
    
def main(argv):
    
    stats_file = ''
    country = 'All'
    res_type = 'All'
    status = 'All'
    org = 'All'
    granularity = 'annually'
    initial_date = ''
    final_date = ''
    stat = ''
    
    statistics = ["NumOfDelegations", "NumOfResources", "IPCount", "IPSpace"]
    res_types = ["All", "asn", "ipv4", "ipv6"]
    statuses = ["All", "available", "reserved", "alloc-32bits", "alloc-16bits",\
                "allocated", "assigned"]
    granularities = ["daily", "weekly", "monthly", "annually"]
    
    try:
        opts, args = getopt.getopt(argv, "hf:c:r:s:o:g:i:e:t:",\
                    ["stats_file=", "country=", "resource_type=", "status=", "org=",\
                    "granularity=", "initial_date=", "final_date=", "stat="])
    except getopt.GetoptError:
        print 'Usage: plotStats.py -h | -f <stats file> -t <statistic> [-c <country>]\
            [-r <resource type>] [-s <status>] [-o <organization>]\
            [-g <granularity>] [-i <initial date>] [-e <final date>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script generates plots from a statistics file."
            print 'Usage: plotStats.py -h | -f <stats file> [-c <country>]\
                [-r <resource type>] [-s <status>] [-o <organization>] [-g <granularity>]'
            print 'h = Help'
            print 'f = CSV file with statistics computed from delegated file. (MANDATORY)'
            print 't = STatistic to be plotted. Choose one from "NumOfDelegations", "NumOfResources",\
                    "IPCount" or "IPSpace". By Default NumOfDelegations will be used.'           
            print 'c = Country code of economy of interest. If no CC is provided, All will be used.'
            print 'r = Resource Type. Choose one resource type from "All", "asn",\
                            "ipv4" or "ipv6"'
            print 's = Status. For Resource Type = All, s = All will be used\
                        no matter what status is given as an input.'
            print 'For Resource Type = asn, choose one status from "All", "available",\
                    "reserved", "alloc-32bits" or "alloc-16bits"'
            print 'For Resource Type = ipv4 or ipv6, choose one status from\
                    "All", "available", "reserved", "allocated" or "assigned"'
            print 'o = Organization of interest. If no org is provided, All will be used.'
            print 'g = Granularity. Choose one granularity from "daily",\
                    "weekly", "monthly" or "annually". If no granularity is provided,\
                    "annually" will be used.'
            print 'i = Initial date for the period of time to be considered. Format: YYYYMMDD'
            print 'e = Final date for the period of time to be considered. Format: YYYYMMDD'

            sys.exit()
        elif opt == '-f':
            stats_file = arg
        elif opt == '-t':
            stat = arg
            if stat not in statistics:
                print "Wrong statistic"
                sys.exit()
        elif opt == '-c':
            country = arg
        elif opt == '-r':
            res_type = arg
            if res_type not in res_types:
                print "Wrong Resource Type"
                sys.exit()
        elif opt == '-s':
            status = arg
            if status not in statuses:
                print "Wrong Status"
                sys.exit()
        elif opt == '-o':
            org = arg
        elif opt == '-g':
            granularity = arg
            if granularity not in granularities:
                print "Wrong granularity"
                sys.exit()
        elif opt == '-i':
            initial_date = arg
        elif opt == '-e':
            final_date = arg
        else:
            assert False, 'Unhandled option'            
        
        if stats_file == '':
            print "The path to a file with statistics MUST be provided.'
            sys.exit()
        
        if stat == '':
            stat = 'NumOfDelegations'
            
        if res_type == 'All':
            status = 'All'
        
        generatePlot(stats_file, country, res_type, status, org, granularity,\
                        initial_date, final_date, stat)
        
if __name__ == "__main__":
    main(sys.argv[1:])