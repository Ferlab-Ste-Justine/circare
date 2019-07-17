# clin-pdf-search
Takes PDFs and outputs their text representation

HOW-TO:

1. Install Tesseract https://github.com/tesseract-ocr/tesseract on system
2. Make sure the tessdata and nlp folders are there (needed for OCR and NLP, respectively)
3. SBT Sync
4. Good to go!

Kibana commands backup

GET /_cat/indices?v

GET /adminword/_search/?size=10000&pretty=1

GET /adminword/_mapping

GET /adminfile/_search?q=*

GET /adminall/_search?q=*

GET /adminfile/_mapping

GET /adminfileword/_settings

GET /adminfilelemma/_mapping

GET /adminfileword/_mapping

GET /adminfileword/_search/?size=10000&pretty=1

GET /adminfilelemma/_search

GET /adminfileword/_search
{
  "size": 0,
    "aggs" : {
        "text" : {
            "terms": {
                "field" : "words.word",
                "size": 100
            },
            "aggs": {
                "best_hits": {
                    "top_hits": {
                      "_source": ["title", "text"], 
                      "size": 10,
                      "highlight": {
                        "fields": {"text": {}}
                      }
                    }
                }
            }
        }
    }
}

GET /adminfileword/_search
{
    "size": 0,
    "aggs" : {
        "text" : {
            "terms": {
                "field" : "words.word",
                "size": 100
            }
        }
    }
}

GET /adminfilelemma/_search
{
    "size": 0,
    "aggs" : {
        "text" : {
            "terms": {
                "field" : "words",
                "size": 1000
            }
        }
    }
}

GET /adminfilelemma/_search
{
    "query" : {
        "match" : {"text" : "MotAChercher"}
    },
    "_source":false,
    "highlight": {
        "fields": {
            "text": {}
        }
    }
}

GET /adminfilelemma/_search
{
  "size": 0,
    "aggs" : {
        "text" : {
            "terms": {
                "field" : "words",
                "size": 100
            },
            "aggs": {
                "best_hits": {
                    "top_hits": {
                      "_source": ["title", "text"], 
                      "size": 10,
                      "highlight": {
                        "fields": {"text": {}}
                      }
                    }
                }
            }
        }
    }
}

GET /adminfile/_search
{
  "query": {
    "match": {
      "text": "MotAChercher" 
    }
  }
}

GET /adminfile/_search
{
    "query" : {
        "match" : {"text" : "MotAChercher"}
    },
    "_source":false,
    "aggregations" : {
        "my_sample" : {
            "sampler" : {
                "shard_size" : 100
            },
            "aggregations": {
                "keywords" : {
                    "significant_text" : { "field" : "text"}
                }
            }
        }
    },
    "highlight": {
        "fields": {
            "text": {}
        }
    }
}

avec le analyser

PUT adminfile/_mapping
{
  "properties": {
    "text" : {
          "type" : "text",
          "fielddata": true,
          "analyzer" : "english"
        }
  }
}

sans le analyser

PUT adminfile/_mapping
{
  "properties": {
    "text" : {
          "type" : "text",
          "fielddata": true,
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        }
  }
}

pour utiliser le hunspell

POST /adminfile/_close

https://qbox.io/blog/elasticsearch-dictionary-stemming-hunspell

PUT /adminfile/_settings
{
    "analysis": {
      "filter": {
        "english_stop": {
          "type": "stop",
          "stopwords": "_english_"
        },
        "length_min": {
               "type": "length",
               "min": 3
        },
        "en_US": {
          "type": "hunspell",
          "language": "en_US" 
        },
        "english_possessive_stemmer": {
          "type": "stemmer",
          "language": "possessive_english" 
        }
      },
      "analyzer": {
        "hunspell_english": {
          "tokenizer": "standard",
          "filter": [
            "english_possessive_stemmer",
            "lowercase",
            "length_min",
            "english_stop",
            "en_US"
          ]
        }
      }
    }
}

PUT adminfile/_mapping
{
  "properties": {
    "text" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          },
          "fielddata": true,
          "analyzer" : "hunspell_english"
        }
  }
}

POST /adminfile/_open

GET /adminfile/_search
{
    "size": 0,
    "aggs" : {
        "text" : {
            "terms" : {
                "field" : "text",
                "order" : { "_count" : "desc" },
                "size": 1000
            }
        }
    }
}

GET /adminfile/_search
{
    "size": 0,
    "aggs" : {
        "text_terms" : {
            "terms" : {
                "field" : "text",
                "order" : { "_count" : "desc" },
                "size": 10
            },
            "aggregations": {
        "related_words" : {
            "significant_text" : {
                "field" : "text",
                "size": 10
            }
        }
    }
        }
    }
}

GET /adminfile/_search
{
    "size": 0,
    "aggs" : {
        "text" : {
          "significant_text" : {
                "field" : "text",
                "filter_duplicate_text": true,
                "size": 10
            }
        }
    }
}

GET /adminfile/_analyze?analyzer=english

GET /adminfile/_search
{
  "aggregations": {
    "text": {
      "terms": {"field": "text"},
        "aggregations": {
          "significant_text_terms": {
          "significant_terms": {"field": "text"}
        }
      }
    }
  }
}

GET /adminfile/_search/?size=1000&pretty=1
{
  "query": {
    "match": {
      "text": "motARechercher"
    }
  }
}

POST /adminfile/_mtermvectors
{
    "ids" : ["NomeDeLetude"],
    "parameters": {
        "fields": [
                "text"
        ],
        "term_statistics": true,
        "filter" : {
      "max_num_terms" : 10,
      "min_term_freq" : 1,
      "min_doc_freq" : 1
    }
    }
}

GET /adminfileword/_search/?size=1000&pretty=1
{
  "query": {
    "match": {
      "text": "motARechercher"
    }
  }
}

POST /adminfileword/_mtermvectors
{
    "ids" : ["NomDeLetude"],
    "parameters": {
        "fields": [
                "text"
        ],
        "term_statistics": true,
        "filter" : {
      "max_num_terms" : 10,
      "min_term_freq" : 1,
      "min_doc_freq" : 1
    }
    }
}

POST /adminall/_mtermvectors
{
    "ids" : ["all"],
    "parameters": {
        "fields": [
                "text"
        ],
        "term_statistics": true,
        "filter" : {
      "max_num_terms" : 10,
      "min_term_freq" : 1,
      "min_doc_freq" : 1
    }
    }
}


DELETE /adminword

DELETE /adminfile

DELETE /adminfilelemma

DELETE /adminfileword

DELETE /adminall

GET /adminword/_search?q=*
