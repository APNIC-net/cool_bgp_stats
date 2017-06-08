mapping = {"_default_" : {
                            "properties" : {
                                "deagg_stat_id" : {
                                    "type": "integer"
                                            },
                                "prefix" : {
                                    "index" : "not_analyzed",
                                    "type": "text"
                                            },
                                "del_date" : {
                                    "type": "date",
                                    "format": "yyyyMMdd"
                                        },
                                "routing_date" : {
                                    "type": "date",
                                    "format": "yyyyMMdd"
                                        },
                                'del_age' : {
                                    "type" : "integer"
                                                    },
                                "isRoot" : {
                                    "type" : "boolean"
                                                    },
                                "isRoootDeagg" : {
                                    "type" : "boolean"
                                                    }
                                        }
                                    }
                                }
                                
index_name = 'deagg_stats_index'
doc_type = 'deagg_stats'
unique_index = ["prefix", "routing_date"]