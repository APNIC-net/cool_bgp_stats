mapping = {"_default_" : {
                            "properties" : {
                                "prefix" : {
                                    "index" : "not_analyzed",
                                    "type": "text"
                                            },
                                "cc" : {
                                    "type" : "text"
                                        },
                                "del_date" : {
                                    "type": "date",
                                    "format": "yyyyMMdd"
                                        },
                                "update_date" : {
                                    "type": "date",
                                    "format": "yyyy-MM-dd"
                                        },
                                'del_age' : {
                                    "type" : "integer"
                                                    },
                                "ip_version" : {
                                    "type" : "integer"
                                                    },
                                "preflen" : {
                                    "type" : "integer"
                                                    },
                                "upd_type" : {
                                    "type" : "text"
                                                    },
                                "updates_count" : {
                                    "type" : "integer"
                                                    }
                                        }
                                    }
                                }
                                
index_name = 'updates_stats_index'
doc_type = 'updates_stats'