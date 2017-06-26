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
    file_date_str = csv_file_name.split('.')[0]
    file_date = datetime.strptime(file_date_str, '%Y-%m-%d').date()
    
    if 'bgpupd' in csv_file_name:   
        readable_folder = '/home/sofia/BGP_stats_files/readable_updates{}'.format(procNumsForYears[file_date.year])
        
        readable_file = '{}/{}.bgpupd.readable'.format(readable_folder, file_date_str)
        if os.path.exists(readable_file):
            os.remove(readable_file)
        
        woSTATE_file = '{}.woSTATE'.format(readable_file)
        if os.path.exists(woSTATE_file):
            os.remove(woSTATE_file)
            
    else:
        log_file = '{}/{}.log'.format(csvs_folder, file_date_str)
        if os.path.exists(log_file):         
            os.remove(log_file)

        filtered_file = '{}.filtered'.format(log_file)
        if os.path.exists(filtered_file):
            os.remove(filtered_file)
            
        announcements_file = '{}.announcements'.format(log_file)
        if os.path.exists(announcements_file):
            os.remove(announcements_file)
            
        withdrawals_file = '{}.withdrawals'.format(log_file)
        if os.path.exists(withdrawals_file):
            os.remove(withdrawals_file)
    