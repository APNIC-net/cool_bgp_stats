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
                                "updates_date" : {
                                    "type": "date",
                                    "format": "yyyyMMdd"
                                        },
                                'del_age' : {
                                    "type" : "integer"
                                                    },
                                "ip_version" : {
                                    "type" : "integer"
                                                    },
                                "prefLength" : {
                                    "type" : "integer"
                                                    },
                                "numOfAnnouncements" : {
                                    "type" : "integer"
                                                    },
                                "numOfWithdraws" : {
                                    "type" : "integer"
                                                    }
                                        }
                                    }
                                }
                                
index_name = 'updates_stats_index'
doc_type = 'updates_stats'
unique_index = ["prefix", "updates_date"]