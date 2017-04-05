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

class VisibilityDBHandler:
    dbname = 'sofia'
    user = 'postgres'
    host = 'localhost'
    password = 'q1w2e3r4'
    routing_date = ''

    def __init__(self, routing_date):
        self.routing_date = routing_date
        
        # Try to connect
        try:
            self.conn = psycopg2.connect("dbname='{}' user='{}' password='{}' host='{}'"\
                                    .format(self.dbname, self.user, self.password, self.host))
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
                             (prefix, date))
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
            
    def storeASSeen(self, asn, isOriginAS, date):
        try:
            self.cur.execute("""INSERT INTO asns VALUES (%s, %s, %s)""",
                             (asn, isOriginAS, date))
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

    def getDateFirstSeen(self, prefix):
        try:
            self.cur.execute("""select dateSeen from prefixes where prefix <<= %s and dateSeen < %s;""",
                             (psycopg2.extras.Inet(prefix), self.routing_date))
            rows = self.cur.fetchall()
            return min(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} or any of its fragments were first seen.\n".format(prefix))
            return None
            
    def getDateFirstSeenExact(self, prefix):
        try:
            self.cur.execute("""select dateSeen from prefixes where prefix = %s and dateSeen < %s;""",
                             (psycopg2.extras.Inet(prefix), self.routing_date))
            rows = self.cur.fetchall()
            return min(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} was first seen\n."\
                            .format(prefix))
            return None
            
    def getPeriodsSeenExact(self, prefix):
        try:
            self.cur.execute("""select dateSeen from prefixes where prefix = %s and dateSeen < %s;""",
                             (psycopg2.extras.Inet(prefix), self.routing_date))
            rows = self.cur.fetchall()
            return self.getListOfDateTuples(rows, True)
        except:
            sys.stderr.write("Unable to get the periods during which the prefix {} was seen.\n".format(prefix))
            return []
            
    def getPeriodsSeenGral(self, prefix):
        try:
            self.cur.execute("""select prefix, dateSeen from prefixes where prefix <<= %s and dateSeen < %s;""",
                             (psycopg2.extras.Inet(prefix), self.routing_date))
            rows = self.cur.fetchall()
            return self.getDictOfPrefixDateTuples(rows)
        except:
            sys.stderr.write("Unable to get the periods during which the prefix {} and its fragments were seen.\n".format(prefix))
            return dict()
            
    def getTotalDaysSeen(self, prefix):
        try:
            self.cur.execute("""select count(dateSeen) from prefixes where prefix <<= %s and dateSeen < %s;""",
                             (psycopg2.extras.Inet(prefix), self.routing_date))
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of days during which the prefix {} or its fragments were seen.\n".format(prefix))
            return -1
                             
    def getTotalDaysSeenExact(self, prefix):
        try:
            self.cur.execute("""select count(dateSeen) from prefixes where prefix = %s and dateSeen < %s;""",
                             (psycopg2.extras.Inet(prefix), self.routing_date))
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of days during which the prefix {} was seen.\n".format(prefix))
            return -1
                             
    def getDateLastSeenExact(self, prefix):
        try:
            self.cur.execute("""select dateSeen from prefixes where prefix = %s and dateSeen < %s;""",
                             (psycopg2.extras.Inet(prefix), self.routing_date))
            rows = self.cur.fetchall()
            return max(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} was last seen.\n"\
                            .format(prefix))
            return None
            
    def getDateLastSeen(self, prefix):
        try:
            self.cur.execute("""select dateSeen from prefixes where prefix <<= %s and dateSeen < %s;""",
                             (psycopg2.extras.Inet(prefix), self.routing_date))
            rows = self.cur.fetchall()
            return max(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} or any of its fragments were last seen.\n".format(prefix))
            return None
        
    def getDateASNFirstSeen(self, asn):
        try:
            self.cur.execute("""select dateSeen from asns where asn = %s and dateSeen < %s;""", (asn, self.routing_date))
            rows = self.cur.fetchall()
            return min(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the ASN {} was first seen.\n"\
                            .format(asn))
            return None
        
    def getPeriodsASNSeen(self, asn):
        try:
            self.cur.execute("""select dateSeen from asns where asn = %s and dateSeen < %s;""", (asn, self.routing_date))
            rows = self.cur.fetchall()
            return self.getListOfDateTuples(rows, True)
        except:
            sys.stderr.write("Unable to get the periods during which the ASN {} was seen.\n".format(asn))
            return []
            
    def getTotalDaysASNSeen(self, asn):
        try:
            self.cur.execute("""select count(*) from (select distinct dateseen from asns where asn = %s and dateSeen < %s) as temp;;""", (asn, self.routing_date))
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of days during which the ASN {} was seen.\n".format(asn))
            return -1
            
    def getDateASNLastSeen(self, asn):
        try:
            self.cur.execute("""select dateSeen from asns where asn = %s and dateSeen < %s;""", (asn, self.routing_date))
            rows = self.cur.fetchall()
            return max(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the ASN {} was last seen.\n".format(asn))
            return None

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
            prefixesPeriodsDict[pref] = VisibilityDBHandler.getListOfDateTuples(\
                                                prefixesPeriodsDict[pref], False)
        
        return prefixesPeriodsDict