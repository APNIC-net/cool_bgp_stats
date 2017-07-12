# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 09:47:06 2017

@author: sofiasilva
"""
import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from iter_file import IteratorFile
import sys, datetime
import psycopg2
import psycopg2.extras
from itertools import chain
import pandas as pd
import sqlalchemy as sq

 # Radix indexed by routed IPv4 prefix containing as values dictionaries
    # with the following keys:
    # * periodsSeen - the value for this key is a list of tuples representing
    # periods of time during which the corresponding IPv4 prefix was seen
    # Each tuple has the format (startDate, endDate)
    # * firstSeen - the value for this key is the first date in which the
    # IPv4 prefix was seen
    # * lastSeen - the value for this key is the last date in which the
    # IPv4 prefix was seen
    # * totalDays - the value for this key is the number of days during
    # which the IPv4 prefix was seen

class DBHandler:
    dbname = 'sofia'
    user = 'postgres'
    host = 'localhost'
    routing_date = ''

    def __init__(self, routing_date=''):
        if routing_date != '':
            self.routing_date = routing_date
        
        # Try to connect
        try:
            # TODO Check if there is any case in which I need to increment timeout.
            # If there is, add: "options='-c statement_timeout=1000'" to connection statement
            self.conn = psycopg2.connect("dbname='{}' user='{}' host='{}'"\
                                    .format(self.dbname, self.user, self.host))
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            psycopg2.extras.register_inet()
        except:
            sys.stderr.write("Unable to connect to the database.\n")
    
    def close(self):
        self.cur.close()
        self.conn.close()

    def storePrefixSeen(self, prefix, date):
        try:
            self.cur.execute("""INSERT INTO prefixes VALUES (%s, %s)""",
                             (prefix, date,))
            self.conn.commit()
            return True
        except psycopg2.IntegrityError:
            self.conn.rollback()
            # Duplicated tuple not inserted into the Prefixes table.
            return False

    def storeListOfPrefixesSeen(self, prefix_list, date):
        tuplesToInsert = zip(prefix_list, [date]*len(prefix_list))
        f = IteratorFile(("{}\t{}".format(x[0], x[1]) for x in tuplesToInsert))
        self.cur.copy_from(f, 'prefixes', columns=('prefix', 'dateseen'))
        self.conn.commit()

            
    def storeASSeen(self, asn, isOriginAS, date):
        try:
            self.cur.execute("""INSERT INTO asns VALUES (%s, %s, %s)""",
                             (asn, isOriginAS, date,))
            self.conn.commit()
            return True
        except psycopg2.IntegrityError:
            self.conn.rollback()
            # Duplicated tuple not inserted into the Prefixes table.
            return False
    
    def storeListOfASesSeen(self, asnsList, areOriginASes, date):
        tuplesToInsert = zip(asnsList, [areOriginASes]*len(asnsList), [date]*len(asnsList))
        f = IteratorFile(("{}\t{}\t{}".format(x[0], x[1], x[2]) for x in tuplesToInsert))
        self.cur.copy_from(f, 'asns', columns=('asn', 'isorigin', 'dateseen'))
        self.conn.commit()

    def getDateFirstSeen(self, prefix):
        try:
            if self.routing_date != '':
                self.cur.execute("""select dateSeen from prefixes where prefix <<= %s and dateSeen < %s;""",
                                 (psycopg2.extras.Inet(prefix), self.routing_date,))
            else:
                self.cur.execute("""select dateSeen from prefixes where prefix <<= %s;""",
                                 (psycopg2.extras.Inet(prefix),))
            rows = self.cur.fetchall()
            return min(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} or any of its fragments were first seen.\n".format(prefix))
            return None
            
    def getDateFirstSeenExact(self, prefix):
        try:
            if self.routing_date != '':
                self.cur.execute("""select dateSeen from prefixes where prefix = %s and dateSeen < %s;""",
                                 (psycopg2.extras.Inet(prefix), self.routing_date,))
            else:
                self.cur.execute("""select dateSeen from prefixes where prefix = %s;""",
                                 (psycopg2.extras.Inet(prefix),))
                                 
            rows = self.cur.fetchall()
            return min(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} was first seen\n."\
                            .format(prefix))
            return None
            
    def getPeriodsSeenExact(self, prefix):
        try:
            if self.routing_date != '':
                self.cur.execute("""select dateSeen from prefixes where prefix = %s and dateSeen < %s;""",
                                 (psycopg2.extras.Inet(prefix), self.routing_date,))
            else:
                self.cur.execute("""select dateSeen from prefixes where prefix = %s;""",
                                 (psycopg2.extras.Inet(prefix),))
                                 
            rows = self.cur.fetchall()
            return self.getListOfDateTuples(rows, True)
        except:
            sys.stderr.write("Unable to get the periods during which the prefix {} was seen.\n".format(prefix))
            return []
            
    def getPeriodsSeenGral(self, prefix):
        try:
            if self.routing_date != '':
                self.cur.execute("""select prefix, dateSeen from prefixes where prefix <<= %s and dateSeen < %s;""",
                                 (psycopg2.extras.Inet(prefix), self.routing_date,))
            else:
                self.cur.execute("""select prefix, dateSeen from prefixes where prefix <<= %s;""",
                                 (psycopg2.extras.Inet(prefix),))
                                 
            rows = self.cur.fetchall()
            return self.getDictOfPrefixDateTuples(rows)
        except:
            sys.stderr.write("Unable to get the periods during which the prefix {} and its fragments were seen.\n".format(prefix))
            return dict()
            
    def getTotalDaysSeen(self, prefix):
        try:
            if self.routing_date != '':
                self.cur.execute("""select count(dateSeen) from prefixes where prefix <<= %s and dateSeen < %s;""",
                                 (psycopg2.extras.Inet(prefix), self.routing_date,))
            else:
                self.cur.execute("""select count(dateSeen) from prefixes where prefix <<= %s;""",
                                 (psycopg2.extras.Inet(prefix),))
                                 
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of days during which the prefix {} or its fragments were seen.\n".format(prefix))
            return -1
                             
    def getTotalDaysSeenExact(self, prefix):
        try:
            if self.routing_date != '':
                self.cur.execute("""select count(dateSeen) from prefixes where prefix = %s and dateSeen < %s;""",
                                 (psycopg2.extras.Inet(prefix), self.routing_date,))
            else:
                self.cur.execute("""select count(dateSeen) from prefixes where prefix = %s;""",
                                 (psycopg2.extras.Inet(prefix),))
                                 
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of days during which the prefix {} was seen.\n".format(prefix))
            return -1
                             
    def getDateLastSeenExact(self, prefix):
        try:
            if self.routing_date != '':
                self.cur.execute("""select dateSeen from prefixes where prefix = %s and dateSeen < %s;""",
                                 (psycopg2.extras.Inet(prefix), self.routing_date,))
            else:
                self.cur.execute("""select dateSeen from prefixes where prefix = %s;""",
                                 (psycopg2.extras.Inet(prefix),))
                                 
            rows = self.cur.fetchall()
            return max(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} was last seen.\n"\
                            .format(prefix))
            return None
            
    def getDateLastSeen(self, prefix):
        try:
            if self.routing_date != '':
                self.cur.execute("""select dateSeen from prefixes where prefix <<= %s and dateSeen < %s;""",
                                 (psycopg2.extras.Inet(prefix), self.routing_date,))
            else:
                self.cur.execute("""select dateSeen from prefixes where prefix <<= %s;""",
                                 (psycopg2.extras.Inet(prefix),))
                                 
            rows = self.cur.fetchall()
            return max(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} or any of its fragments were last seen.\n".format(prefix))
            return None
        
    def getDateASNFirstSeen(self, asn):
        try:
            if self.routing_date != '':
                self.cur.execute("""select dateSeen from asns where asn = %s and dateSeen < %s;""", (asn, self.routing_date,))
            else:
                self.cur.execute("""select dateSeen from asns where asn = %s;""", (asn,))
                
            rows = self.cur.fetchall()
            return min(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the ASN {} was first seen.\n"\
                            .format(asn))
            return None
        
    def getPeriodsASNSeen(self, asn):
        try:
            if self.routing_date != '':
                self.cur.execute("""select dateSeen from asns where asn = %s and dateSeen < %s;""", (asn, self.routing_date,))
            else:
                self.cur.execute("""select dateSeen from asns where asn = %s;""", (asn,))
                
            rows = self.cur.fetchall()
            return self.getListOfDateTuples(rows, True)
        except:
            sys.stderr.write("Unable to get the periods during which the ASN {} was seen.\n".format(asn))
            return []
            
    def getTotalDaysASNSeen(self, asn):
        try:
            if self.routing_date != '':
                self.cur.execute("""select count(*) from (select distinct dateseen from asns where asn = %s and dateSeen < %s) as temp;""", (asn, self.routing_date,))
            else:
                self.cur.execute("""select count(*) from (select distinct dateseen from asns where asn = %s) as temp;""", (asn,))
                
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of days during which the ASN {} was seen.\n".format(asn))
            return -1
            
    def getDateASNLastSeen(self, asn):
        try:
            if self.routing_date != '':
                self.cur.execute("""select dateSeen from asns where asn = %s and dateSeen < %s;""", (asn, self.routing_date,))
            else:
                self.cur.execute("""select dateSeen from asns where asn = %s;""", (asn,))
                
            rows = self.cur.fetchall()
            return max(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the ASN {} was last seen.\n".format(asn))
            return None
    
    def getPrefixCountForDate(self, date):
        try:
            self.cur.execute("""select count(*) from prefixes where dateSeen = %s;""", (date,))
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of prefixes seen on {}\n".format(date))
            return -1
            
    def getOriginASCountForDate(self, date):
        try:
            self.cur.execute("""select count(*) from asns where isorigin = %s and dateSeen = %s;""", (True, date,))
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of origin ASes seen on {}\n".format(date))
            return -1        
    
    def getMiddleASCountForDate(self, date):
        try:
            self.cur.execute("""select count(*) from asns where isorigin = %s and dateSeen = %s;""", (False, date,))
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of middle ASes seen on {}\n".format(date))
            return -1
    
    def dropPrefixesForDate(self, date):
        try:
            self.cur.execute("""DELETE FROM prefixes WHERE dateseen = %s""",
                             (date,))
            self.conn.commit()
            return True
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False

    def dropOriginASesForDate(self, date):
        try:
            self.cur.execute("""DELETE FROM asns WHERE isorigin = %s and  dateseen = %s""",
                             (True, date,))
            self.conn.commit()
            return True
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False

    def dropMiddleASesForDate(self, date):
        try:
            self.cur.execute("""DELETE FROM asns WHERE isorigin = %s and  dateseen = %s""",
                             (False, date,))
            self.conn.commit()
            return True
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
    
    def getListOfDatesForPrefixes(self):
        try:
            self.cur.execute("SELECT distinct(dateseen) from prefixes")
            return list(chain(*self.cur.fetchall()))
        except:
            sys.stderr.write("Unable to get the list of distinct dates for the prefixes in the DB.\n")
            return []

    def getListOfDatesForOriginASes(self):
        try:
            self.cur.execute("SELECT distinct(dateseen) from asns where isorigin = True")
            return list(chain(*self.cur.fetchall()))
        except:
            sys.stderr.write("Unable to get the list of distinct dates for the origin ASes in the DB.\n")
            return []

    def getListOfDatesForMiddleASes(self):
        try:
            self.cur.execute("SELECT distinct(dateseen) from asns where isorigin = False")
            return list(chain(*self.cur.fetchall()))
        except:
            sys.stderr.write("Unable to get the list of distinct dates for the middle ASes in the DB.\n")
            return []
    
    def getListOfDatesForUpdates(self):
        try:
            self.cur.execute("SELECT distinct(update_date) from updates")
            return list(chain(*self.cur.fetchall()))
        except:
            sys.stderr.write("Unable to get the list of distinct dates for the updates in the DB.\n")
            return []
    
    def getListOfDatesForRoutingData_v4Only(self):
        try:
            self.cur.execute("SELECT distinct(routing_date) from routing_data where extension = 'dmp.gz'")
            return list(chain(*self.cur.fetchall()))
        except:
            sys.stderr.write("Unable to get the list of distinct dates for v4 only routing data in the DB.\n")
            return []
    
    def getListOfDatesForRoutingData_v6Only(self):
        try:
            self.cur.execute("SELECT distinct(routing_date) from routing_data where extension = 'v6.dmp.gz'")
            return list(chain(*self.cur.fetchall()))
        except:
            sys.stderr.write("Unable to get the list of distinct dates for v6 only routing data in the DB.\n")
            return []
    
    def getListOfDatesForRoutingData_v4andv6(self):
        try:
            self.cur.execute("SELECT distinct(routing_date) from routing_data where extension = 'bgprib.mrt'")
            return list(chain(*self.cur.fetchall()))
        except:
            sys.stderr.write("Unable to get the list of distinct dates for v4 and v6 routing data in the DB.\n")
            return []
            
    @staticmethod    
    def getListOfDateTuples(datesList, isDict):
        periodsList = []
        
        datesList.sort()
        lastPeriodsListIndex = -1
        
        for i in range(len(datesList)):
            if isDict:
                currentDate = datesList[i]['dateseen']
            else:
                currentDate = datesList[i]
            if i > 0 and currentDate == periodsList[lastPeriodsListIndex][1] + datetime.timedelta(days=1):
                periodsList[lastPeriodsListIndex] = (periodsList[lastPeriodsListIndex][0], currentDate)
            else:
                periodsList.append((currentDate, currentDate))
                lastPeriodsListIndex += 1
        
        return periodsList
    
    @staticmethod
    def getDictOfPrefixDateTuples(prefixesDates):
        prefixesPeriodsDict = dict()
        
        prefixesDates.sort()
        
        for prefDate in prefixesDates:
            if prefDate['prefix'] not in prefixesPeriodsDict:
                prefixesPeriodsDict[prefDate['prefix']] = [prefDate['dateseen']]
            else:
                prefixesPeriodsDict[prefDate['prefix']].append(prefDate['dateseen'])
        
        for pref in prefixesPeriodsDict:
            prefixesPeriodsDict[pref] = DBHandler.getListOfDateTuples(\
                                                prefixesPeriodsDict[pref], False)
        
        return prefixesPeriodsDict
        
    # This function returns a dictionary indexed by date containing the paths
    # to the files in the archive folder for the dates within the provided
    # period of time.
    def getPathsToRoutingFilesForPeriod(self, startDate, endDate):
        try:
            self.cur.execute("""SELECT routing_date, extension, file_path from routing_data 
                                where routing_date between %s and %s""",
                                (startDate, endDate,))
                                
            result_list = self.cur.fetchall()

            routing_files = dict()            
            for item in result_list:
                # We have a list of lists of the form:
                # [[date, extension, path], [date, extension, path], ...]
                routing_files[item[0]] = dict()
                routing_files[item[0]][item[1]] = item[2]
            
            return routing_files
        except:
            sys.stderr.write("Unable to get the list of paths to routing files for the period {}-{}.\n".format(startDate, endDate))
            return dict()
        
    # This function returns a list of paths to the routing files
    # in the archive corresponding to the provided date.
    def getPathsToRoutingFilesForDate(self, routing_date):
        try:
            self.cur.execute("""SELECT extension, file_path from routing_data 
                                where routing_date = %s""", (routing_date,))
                                
            result_list = self.cur.fetchall()
            
            routing_files = dict()
            for item in result_list:
                # We have a list of lists of the form:
                # [[extension, file_path], [extension, file_path],...]
                routing_files[item[0]] = item[1]
            
            return routing_files

        except:
            sys.stderr.write("Unable to get the list of paths to routing files for date {}.\n".format(routing_date))
            return dict()
            
    def getPathsToMostRecentRoutingFiles(self):
        try:
            self.cur.execute("""SELECT distinct(routing_date) from routing_data 
                                order by routing_date desc limit 1""")

            mostRecentDate = self.cur.fetchone()[0]
            
            return self.getPathsToRoutingFilesForDate(mostRecentDate)

        except:
            sys.stderr.write("Unable to get the list of paths to most recent \
                                routing files\n")
            return dict()
            
    def getUpdatesDF(self, updates_date):
        try:
            engine = sq.create_engine("postgresql+psycopg2://{}@{}/{}"\
                                    .format(self.user, self.host, self.dbname))
            
            return pd.read_sql_query("SELECT update_date, upd_type, peerAS, prefix FROM updates where update_date = {};".format(updates_date), engine)

        except:
            sys.stderr.write("Unable to get updates DataFrame for date {}.\n".format(updates_date))
            return pd.DataFrame()