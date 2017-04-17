mapping = {"_default_" : {
                            "properties" : {
                                "pref_stat_id" : {
                                    "type": "integer"
                                            },
                                "prefix" : {
                                    "index" : "not_analyzed",
                                    "type": "text"
                                            },
                                "resource_type" : {
                                    "index" : "not_analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "status" : {
                                    "index" : "not_analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "opaque_id" : {
                                    "index" : "not_analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "cc" : {
                                    "index" : "not_analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "region" : {
                                    "index" : "not_analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "del_date" : {
                                    "type": "date",
                                    "format": "yyyyMMdd"
                                        },
                                "routing_date" : {
                                    "type": "date",
                                    "format": "yyyyMMdd"
                                        },
                                "lastSeen" : {
                                    "type": "date",
                                    "format": "yyyyMMdd"
                                        },
                                "lastSeenIntact" : {
                                    "type": "date",
                                    "format": "yyyyMMdd"
                                        },
                                "isRoutedIntact" : {
                                    "type" : "boolean"
                                                    },
                                'isDead' : {
                                    "type" : "boolean"
                                                    },
                                'isDeadIntact' : {
                                    "type" : "boolean"
                                                    },
                                'originatedByDiffOrg' : {
                                    "type" : "boolean"
                                                    },
                                'hasFragmentsOriginatedByDiffOrg' : {
                                    "type" : "boolean"
                                                    },
                                'hasLessSpecificsOriginatedByDiffOrg' : {
                                    "type" : "boolean"
                                                    },
                                'onlyRoot' : {
                                    "type" : "boolean"
                                                    },
                                'rootMSCompl' : {
                                    "type" : "boolean"
                                                    },
                                'rootMSIncompl' : {
                                    "type" : "boolean"
                                                    },
                                'noRootMSCompl' : {
                                    "type" : "boolean"
                                                    },
                                'noRootMSIncompl' : {
                                    "type" : "boolean"
                                                    },
                                'isLonely' : {
                                    "type" : "boolean"
                                                    },
                                'isSOSP' : {
                                    "type" : "boolean"
                                                    },
                                'isDODP1' : {
                                    "type" : "boolean"
                                                    },
                                'isDODP2'  : {
                                    "type" : "boolean"
                                                    },
                                'isDODP3' : {
                                    "type" : "boolean"
                                                    },
                                'isCoveredLevel2plus' : {
                                    "type" : "boolean"
                                                    },
                                'isDOSP' : {
                                    "type" : "boolean"
                                                    },
                                'isCoveredLevel1' : {
                                    "type" : "boolean"
                                                    },
                                'isSODP2' : {
                                    "type" : "boolean"
                                                    },
                                'isCovering' : {
                                    "type" : "boolean"
                                                    },
                                'isSODP1' : {
                                    "type" : "boolean"
                                                    },
                                'UsageLatencyGral' : {
                                    "type" : "integer"
                                                    },
                                'UsageLatencyIntact' : {
                                    "type" : "integer"
                                                    },
                                'relUsedTimeIntact' : {
                                    "type" : "float"
                                                    },
                                'avgRelUsedTimeGral' : {
                                    "type" : "float"
                                                    },
                                'effectiveUsageIntact' : {
                                    "type" : "float"
                                                    },
                                'avgEffectiveUsageGral' : {
                                    "type" : "float"
                                                    },
                                'timeFragmentationIntact' : {
                                    "type" : "float"
                                                    },
                                'avgTimeFragmentationGral' : {
                                    "type" : "float"
                                                    },
                                'avgPeriodLengthIntact' : {
                                    "type" : "float"
                                                    },
                                'stdPeriodLengthIntact' : {
                                    "type" : "float"
                                                    },
                                'maxPeriodLengthIntact' : {
                                    "type" : "float"
                                                    },
                                'minPeriodLengthIntact' : {
                                    "type" : "float"
                                                    },
                                'avgPeriodLengthGral' : {
                                    "type" : "float"
                                                    },
                                'stdPeriodLengthGral' : {
                                    "type" : "float"
                                                    },
                                'maxPeriodLengthGral' : {
                                    "type" : "float"
                                                    },
                                'minPeriodLengthGral' : {
                                    "type" : "float"
                                                    },
                                'avgVisibility' : {
                                    "type" : "float"
                                                    },
                                'stdVisibility' : {
                                    "type" : "float"
                                                    },
                                'maxVisibility' : {
                                    "type" : "float"
                                                    },
                                'minVisibility' : {
                                    "type" : "float"
                                                    },
                                'avgASPathLengthIntact' : {
                                    "type" : "float"
                                                    },
                                'stdASPathLengthIntact' : {
                                    "type" : "float"
                                                    },
                                'maxASPathLengthIntact' : {
                                    "type" : "float"
                                                    },
                                'minASPathLengthIntact' : {
                                    "type" : "float"
                                                    },
                                'avgNumOfOriginASesMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'stdNumOfOriginASesMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'minNumOfOriginASesMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'maxNumOfOriginASesMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'avgNumOfASPathsMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'stdNumOfASPathsMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'minNumOfASPathsMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'maxNumOfASPathsMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'avgASPathLengthMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'stdASPathLengthMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'minASPathLengthMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'maxASPathLengthMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'avgNumOfOriginASesLessSpec' : {
                                    "type" : "float"
                                                    },
                                'stdNumOfOriginASesLessSpec' : {
                                    "type" : "float"
                                                    },
                                'minNumOfOriginASesLessSpec' : {
                                    "type" : "float"
                                                    },
                                'maxNumOfOriginASesLessSpec' : {
                                    "type" : "float"
                                                    },
                                'avgNumOfASPathsLessSpec' : {
                                    "type" : "float"
                                                    },
                                'stdNumOfASPathsLessSpec' : {
                                    "type" : "float"
                                                    },
                                'minNumOfASPathsLessSpec' : {
                                    "type" : "float"
                                                    },
                                'maxNumOfASPathsLessSpec' : {
                                    "type" : "float"
                                                    },
                                'avgASPathLengthLessSpec' : {
                                    "type" : "float"
                                                    },
                                'stdASPathLengthLessSpec' : {
                                    "type" : "float"
                                                    },
                                'minASPathLengthLessSpec' : {
                                    "type" : "float"
                                                    },
                                'maxASPathLengthLessSpec' : {
                                    "type" : "float"
                                                    },
                                'avgLevenshteinDistPrefix' : {
                                    "type" : "float"
                                                    },
                                'stdLevenshteinDistPrefix' : {
                                    "type" : "float"
                                                    },
                                'minLevenshteinDistPrefix' : {
                                    "type" : "float"
                                                    },
                                'maxLevenshteinDistPrefix' : {
                                    "type" : "float"
                                                    },
                                'avgLevenshteinDistMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'stdLevenshteinDistMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'minLevenshteinDistMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'maxLevenshteinDistMoreSpec' : {
                                    "type" : "float"
                                                    },
                                'avgLevenshteinDistLessSpec' : {
                                    "type" : "float"
                                                    },
                                'stdLevenshteinDistLessSpec' : {
                                    "type" : "float"
                                                    },
                                'minLevenshteinDistLessSpec' : {
                                    "type" : "float"
                                                    },
                                'maxLevenshteinDistLessSpec' : {
                                    "type" : "float"
                                                    },
                                'currentVisibility' : {
                                    "type" : "float"
                                                    },
                                'numOfOriginASesIntact' : {
                                    "type" : "integer"
                                                    },
                                'numOfASPathsIntact' : {
                                    "type" : "integer"
                                                    },
                                'numOfLessSpecificsRouted' : {
                                    "type" : "integer"
                                                    },
                                'numOfMoreSpecificsRouted' : {
                                    "type" : "integer"
                                                    },
                                'numOfLonelyMoreSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfSOSPMoreSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfDODP1MoreSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfDODP2MoreSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfDODP3MoreSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfCoveredLevel2plusMoreSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfDOSPMoreSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfCoveredLevel1MoreSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfSODP2MoreSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfCoveringMoreSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfSODP1MoreSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfLonelyLessSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfSOSPLessSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfDODP1LessSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfDODP2LessSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfDODP3LessSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfCoveredLevel2plusLessSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfDOSPLessSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfCoveredLevel1LessSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfSODP2LessSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfCoveringLessSpec' : {
                                    "type" : "integer"
                                                    },
                                'numOfSODP1LessSpec' : {
                                    "type" : "integer"
                                                    }
                                        }
                                    }
                                }
                                
index_name = 'prefix_stats_index'
doc_type = 'prefix_stats'
unique_index = ["prefix", "routing_date"]