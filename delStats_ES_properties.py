mapping = {"_default_" : {
                            "properties" : {
                                "del_stat_id" : {
                                    "type": "integer"
                                            },
                                "GeographicArea" : {
                                    "index" : "analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "ResourceType" : {
                                    "index" : "not_analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "Status" : {
                                    "index" : "not_analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "Organization" : {
                                    "index" : "not_analyzed",
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                                }}},
                                "Date" : {
                                    "type": "date",
                                    "format": "yyyyMMdd"
                                        },
                                "NumOfDelegations" : {
                                    "type" : "integer"
                                                    },
                                "NumOfResources" : {
                                    "type" : "integer"
                                                    },
                                "IPCount" : {
                                    "type" : "long"
                                            },
                                "IPSpace" : {
                                    "type" : "long"
                                            }
                                        }
                                    }
                                }
index_name = 'delegated_stats_index'
doc_type = 'delegated_stats'