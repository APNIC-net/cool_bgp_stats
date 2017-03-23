# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 09:47:06 2017

@author: sofiasilva
"""
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

    def __init__(self):
        # Try to connect
        try:
            conn = psycopg2.connect("dbname='{}' user='{}' host='{}'"\
                                    .format(self.dbname, self.user, self.host))
            self.cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            psycopg2.extras.register_inet()
        except:
            sys.stderr.write("Unable to connect to the database.")

    def storePrefixSeen(self, prefix, date):
        try:
            self.cur.execute("""INSERT INTO prefixes VALUES (%s, %s)""",
                             (prefix, date))
            return True
        except:
            sys.stderr.write("Unable to insert ({}, {}) into the Prefixes table."\
                            .format(prefix, date))
            return False
            
    def storeASSeen(self, asn, isOriginAS, date):
        try:
            self.cur.execute("""INSERT INTO asns VALUES (%s, %s, %s)""",
                             (asn, isOriginAS, date))
            return True
        except:
            sys.stderr.write("Unable to insert ({}, {}, {}) into the ASNs table."\
                            .format(asn, isOriginAS, date))
            return False
            
    def getDateFirstSeen(self, prefix):
        try:
            self.cur.execute("""select dateSeen from prefixes where prefix <<= %s;""",
                             (psycopg2.extras.Inet(prefix),))
            rows = self.cur.fetchall()
            return min(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} or any of its\
                            fragments were first seen.".format(prefix))
            return None
            
    def getDateFirstSeenExact(self, prefix):
        try:
            self.cur.execute("""select dateSeen from prefixes where prefix = %s;""",
                             (psycopg2.extras.Inet(prefix),))
            rows = self.cur.fetchall()
            return min(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} was first seen."\
                            .format(prefix))
            return None
            
    def getPeriodsSeenExact(self, prefix):
        try:
            self.cur.execute("""select dateSeen from prefixes where prefix = %s;""",
                             (psycopg2.extras.Inet(prefix),))
            rows = self.cur.fetchall()
            return self.getListOfDateTuples(rows, True)
        except:
            sys.stderr.write("Unable to get the periods during which the prefix\
                            {} was seen.".format(prefix))
            return None
            
    def getPeriodsSeenGral(self, prefix):
        try:
            self.cur.execute("""select prefix, dateSeen from prefixes where prefix <<= %s;""",
                             (psycopg2.extras.Inet(prefix),))
            rows = self.cur.fetchall()
            return self.getDictOfPrefixDateTuples(rows)
        except:
            sys.stderr.write("Unable to get the periods during which the prefix\
                            {} and its fragments were seen.".format(prefix))
            return None
            
    def getTotalDaysSeen(self, prefix):
        try:
            self.cur.execute("""select count(dateSeen) from prefixes where prefix <<= %s;""",
                             (psycopg2.extras.Inet(prefix),))
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of days during which the\
                            prefix {} or its fragments were seen.".format(prefix))
            return -1
                             
    def getTotalDaysSeenExact(self, prefix):
        try:
            self.cur.execute("""select count(dateSeen) from prefixes where prefix = %s;""",
                             (psycopg2.extras.Inet(prefix),))
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of days during which the\
                            prefix {} was seen.".format(prefix))
            return -1
                             
    def getDateLastSeenExact(self, prefix):
        try:
            self.cur.execute("""select dateSeen from prefixes where prefix = %s;""",
                             (psycopg2.extras.Inet(prefix),))
            rows = self.cur.fetchall()
            return max(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} was last seen."\
                            .format(prefix))
            return None
            
    def getDateLastSeen(self, prefix):
        try:
            self.cur.execute("""select dateSeen from prefixes where prefix <<= %s;""",
                             (psycopg2.extras.Inet(prefix),))
            rows = self.cur.fetchall()
            return max(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the prefix {} or any of its\
                            fragments were last seen.".format(prefix))
            return None
        
    def getDateASNFirstSeen(self, asn):
        try:
            self.cur.execute("""select dateSeen from asns where asn = %s;""", (asn,))
            rows = self.cur.fetchall()
            return min(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the ASN {} was first seen."\
                            .format(asn))
            return None
        
    def getPeriodsASNSeen(self, asn):
        try:
            self.cur.execute("""select dateSeen from asns where asn = %s;""", (asn,))
            rows = self.cur.fetchall()
            return self.getListOfDateTuples(rows, True)
        except:
            sys.stderr.write("Unable to get the periods during which the ASN\
                            {} was seen.".format(asn))
            return None
            
    def getTotalDaysASNSeen(self, asn):
        try:
            self.cur.execute("""select count(*) from (select distinct dateseen from asns where asn = %s) as temp;;""", (asn,))
            return self.cur.fetchone()[0]
        except:
            sys.stderr.write("Unable to get the number of days during which the\
                            ASN {} was seen.".format(asn))
            return -1
            
    def getDateASNLastSeen(self, asn):
        try:
            self.cur.execute("""select dateSeen from asns where asn = %s;""", (asn,))
            rows = self.cur.fetchall()
            return max(rows)['dateseen']
        except:
            sys.stderr.write("Unable to get the date the ASN {} was last seen.".format(asn))
            return None

    @classmethod    
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
    
    @classmethod
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