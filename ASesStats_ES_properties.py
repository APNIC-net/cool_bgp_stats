mapping = {"_default_" : {
                            "properties" : {
                                "asn_stat_id" : {
                                    "type": "integer"
                                            },
                                "asn" : {
                                    "index" : "not_analyzed",
                                    "type": "long"
                                            },
                                "asn_type" : {
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
                                'isDead' : {
                                    "type" : "boolean"
                                                    },
                                'UsageLatency' : {
                                    "type" : "integer"
                                                    },
                                'relUsedTime' : {
                                    "type" : "float"
                                                    },
                                'effectiveUsage' : {
                                    "type" : "float"
                                                    },
                                'timeFragmentation' : {
                                    "type" : "float"
                                                    },
                                'avgPeriodLength' : {
                                    "type" : "float"
                                                    },
                                'stdPeriodLength' : {
                                    "type" : "float"
                                                    },
                                'maxPeriodLength' : {
                                    "type" : "float"
                                                    },
                                'minPeriodLength' : {
                                    "type" : "float"
                                                    },
                                'numOfPrefixesOriginated' : {
                                    "type" : "integer"
                                                    },
                                'numOfPrefixesPropagated' : {
                                    "type" : "integer"
                                                    }
                                        }
                                    }
                                }
                                
index_name = 'ases_stats_index'
doc_type = 'ases_stats'
unique_index = ["asn", "routing_date"]