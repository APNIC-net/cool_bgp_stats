# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 09:09:37 2017

@author: sofiasilva
"""
import os
from glob import glob
from datetime import datetime

# Script to clean the files used to generate the CSV files for the updates files

csvs_folder = '/home/sofia/BGP_stats_files/Updates_files'

procNumsForYears = {2007:1, 2008:1, 2009:1, 2010:2, 2011:2, 2012:3, 2013:3, 2014:4,
            2015:4, 2016:5, 2017:5}

for csv_file in glob('{}/*.csv'.format(csvs_folder)):
    csv_file_name = csv_file.split('/')[-1]
    file_date = datetime.strptime(csv_file_name.split('.')[0], '%Y-%m-%d')
    
    if 'bgpupg' in csv_file_name:   
        readable_folder = '/home/sofia/BGP_stats_files/readable_updates{}'.format(procNumsForYears[file_date.year])
        
        os.remove('{}/{}.bgpupd.readable'.format(readable_folder, file_date))
        os.remove('{}/{}.bgpupd.readable.woSTATE'.format(readable_folder, file_date))
    else:
       os.remove('{}/{}.log'.format(csvs_folder, file_date))
       os.remove('{}/{}.log.filtered'.format(csvs_folder, file_date)) 
       os.remove('{}/{}.log.announcements'.format(csvs_folder, file_date)) 
       os.remove('{}/{}.log.withdrawals'.format(csvs_folder, file_date))
    
    
    