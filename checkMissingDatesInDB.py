# -*- coding: utf-8 -*-
"""
Created on Wed Jul 12 17:24:35 2017

@author: sofiasilva
"""
import os
os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from DBHandler import DBHandler
from datetime import date, timedelta

initial_date = date(2007, 6, 11)
final_date = date.today()
numOfDays = (final_date - initial_date).days
complete_dates_set = set([final_date - timedelta(days=x) for x in range(0, numOfDays)])

db_handler = DBHandler('')

db_pref_dates = set(db_handler.getListOfDatesForPrefixes())
missing_pref = complete_dates_set - db_pref_dates

db_originASes_dates = set(db_handler.getListOfDatesForOriginASes())
missing_originASes = complete_dates_set - db_originASes_dates

db_middleASes_dates = set(db_handler.getListOfDatesForMiddleASes())
missing_middleASes = complete_dates_set - db_middleASes_dates

db_routing_data_v4_dates = set(db_handler.getListOfDatesForRoutingData_v4Only())
missing_routing_v4 = complete_dates_set - db_routing_data_v4_dates

db_routing_data_v6_dates = set(db_handler.getListOfDatesForRoutingData_v6Only())
missing_routing_v6 = complete_dates_set - db_routing_data_v6_dates

db_routing_data_v4andv6_dates = set(db_handler.getListOfDatesForRoutingData_v4andv6())
missing_routing_v4andv6 = complete_dates_set - db_routing_data_v4andv6_dates

db_updates_dates = set(db_handler.getListOfDatesForUpdates())
missing_updates = complete_dates_set - db_updates_dates

db_handler.close()

print "Dates missing in the DB"
print "{} dates missing for prefixes.".format(len(missing_pref))
print missing_pref
print "{} dates missing for origin ASes.".format(len(missing_originASes))
print missing_originASes
print "{} dates missing for middle ASes.".format(len(missing_middleASes))
print missing_middleASes
print "{} dates missing for v4 routing data.".format(len(missing_routing_v4))
print missing_routing_v4
print "{} dates missing for v6 routing data.".format(len(missing_routing_v6))
print missing_routing_v6
print "{} dates missing for v4andv6 routing data.".format(len(missing_routing_v4andv6))
print missing_routing_v4andv6
print "{} dates missing for updates.".format(len(missing_updates))
print missing_updates