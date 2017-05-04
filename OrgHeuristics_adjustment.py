# -*- coding: utf-8 -*-
"""
Created on Thu May  4 14:48:23 2017

@author: sofiasilva
"""
import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
#Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from OrgHeuristics import OrgHeuristics

# For debug in matong
#org_h = OrgHeuristics('/Users/sofiasilva/Downloads')

# For local debug
org_h = OrgHeuristics('/home/sofia/BGP_stats_files')

correctResults = 0
falsePositives = 0
falseNegatives = 0

falseNeg_file = './falseNegatives.csv'
with open(falseNeg_file, 'wb') as f:
    f.write('Prefix|ASN|Matchings\n')
    
falsePos_file = './falsePositives.csv'
with open(falsePos_file, 'wb') as f:
    f.write('Prefix|ASN|Matchings\n')
    
sameOrgPairs = [['1.0.64.0/18', '18144'],
                ['103.15.226.0/24', '136052'],
                ['45.121.52.0/22', '63530'],
                ['103.52.223.0/24', '133961'],
                ['43.255.12.0/22', '131584'],
                ['103.84.212.0/22', '136317'],
                ['103.86.180.0/22', '136284'],
                ['103.86.190.0/24', '136413'],
                ['103.86.191.0/24', '136414'],
                ['2400:c740::/32', '136416'],
                ['103.86.196.0/22', '18109'],
                ['103.87.24.0/22', '136287'],
                ['103.87.28.0/22', '136288']]

for sameOrg_pair in sameOrgPairs:
    matchings = []
    result = org_h.checkIfSameOrg(sameOrg_pair[0], sameOrg_pair[1], matchings)
    
    if result:
        correctResults += 1
    else:
        falseNegatives += 1
        with open(falseNeg_file, 'a') as f:
            f.write('{}|{}|{}\n'.format(sameOrg_pair[0], sameOrg_pair[1], matchings))
    
diffOrgPairs = [['1.0.64.0/18', '136288'],
                ['103.15.226.0/24', '18109'],
                ['45.121.52.0/22', '136288'],
                ['103.52.223.0/24', '136414'],
                ['43.255.12.0/22', '136413'],
                ['211.190.231.0/24', '38660'],
                ['103.84.212.0/22', '136290'],
                ['103.86.180.0/22', '136372'],
                ['103.86.190.0/24', '131491'],
                ['103.86.191.0/24', '136370'],
                ['2400:c740::/32', '136427'],
                ['103.86.196.0/22', '136091'],
                ['103.87.24.0/22', '45566'],
                ['103.87.28.0/22', '136433']]

for diffOrg_pair in diffOrgPairs:
    matchings = []
    result = org_h.checkIfSameOrg(diffOrg_pair[0], diffOrg_pair[1], matchings)
    
    if not result:
        correctResults += 1
    else:
        falsePositives += 1
        with open(falsePos_file, 'a') as f:
            f.write('{}|{}|{}\n'.format(diffOrg_pair[0], diffOrg_pair[1], matchings))

print 'Correct results: {}%'.format(100*float(correctResults)/(len(sameOrgPairs)+len(diffOrgPairs)))
print 'False positives: {}%'.format(100*float(falsePositives)/(len(sameOrgPairs)+len(diffOrgPairs)))
print 'False negatives: {}%'.format(100*float(falseNegatives)/(len(sameOrgPairs)+len(diffOrgPairs)))
