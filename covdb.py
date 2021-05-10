#!/usr/bin/env python

#
# covid 19 research network
# written by Armin Pournaki and Alexander Dejaco
# in part funded by the European Open Science Cloud
#
# under GPL License
#

import psycopg2
import json
import sys
import code

from helpers import *

def cmd(sql):
   try:
      db = psycopg2.connect(host="localhost", database=PSQL_DBNAME, user=PSQL_USER, password=PSQL_PASS)
      cur = db.cursor()
      cur.execute(sql)
      db.commit()
   except (Exception, psycopg2.DatabaseError) as error:
      print("exception: ", error)
   finally:
      if(db):
         cur.close()
         db.close()


def merge():
   try:
      db = psycopg2.connect(host="localhost", database=PSQL_DBNAME, user=PSQL_USER, password=PSQL_PASS)
      cur = db.cursor()

#      sql="DROP TABLE IF EXISTS all_articles"
#      cur.execute(sql)
#      db.commit()

      sql=("insert into all_articles (doc_id, doi, server, url, date, title, authors, abstract, nlp_lemma_text, nlp_lemma_pos, nlp_lemmatization_by, nlp_lang, journal) select pmid, doi, server, url, date, title, authors, abstract, nlp_lemma_text, nlp_lemma_pos, nlp_lemmatization_by, nlp_lang, journal from pubmed_articles where title!='' and recheck='0'")
      cur.execute(sql)
      db.commit()

      sql=("insert into all_articles (doc_id, doi, server, url, date, title, authors, abstract, nlp_lemma_text, nlp_lemma_pos, nlp_lemmatization_by, nlp_lang, journal) select doc_id, doi, server, url, date, title, authors, abstract, nlp_lemma_text, nlp_lemma_pos, nlp_lemmatization_by, nlp_lang, server from preprint_articles")

      cur.execute(sql)
      db.commit()
            
   
   except (Exception, psycopg2.DatabaseError) as error:
      print("exception: ", error)
   finally:
      if(db):
         cur.close()
         db.close()

# delete database, create new one
def create():   
   try:
      db = psycopg2.connect(host="localhost", database=PSQL_DBNAME, user=PSQL_USER, password=PSQL_PASS)
      cur = db.cursor()

      # delete table
      sql=("""DROP TABLE IF EXISTS topics_per_article""")
      cur.execute(sql)
      db.commit()
      sql=("""DROP TABLE IF EXISTS pubmed_articles""")
      cur.execute(sql)
      db.commit()
      sql=("""DROP TABLE IF EXISTS preprint_articles""")
      cur.execute(sql)
      db.commit()
      sql=("""DROP TABLE IF EXISTS all_articles""")
      cur.execute(sql)
      db.commit()
      sql=("""DROP TABLE IF EXISTS duplicates""")
      cur.execute(sql)
      db.commit()
      sql=("""DROP TABLE IF EXISTS emptytitles""")
      cur.execute(sql)
      db.commit()

      
      print("deleted old tables")

      sql=("""create table if not exists all_articles (
        id int generated always as identity primary key,
        doc_id text not null unique,
        doi text,
        server text not null,
        url text not null,
        date date not null,
        title text not null,
        authors text not null,
        abstract text not null,
        nlp_lemma_text text[] not null,
        nlp_lemma_pos text[] not null,
        nlp_lemmatization_by text not null,
        nlp_lang float,
        journal text not null
      )""")
      cur.execute(sql)
      db.commit()

      # create table
      sql=("""create table pubmed_articles (
            pmid int primary key,
            title text not null,
            abstract text,
            journal text not null,
            date date,
            authors text not null,
            doi text,
            url text not null,
            server text not null,
            recheck int not null,
            nlp_lemma_text text[] not null,
            nlp_lemma_pos text[] not null,
            nlp_lemmatization_by text not null,
            nlp_lang float
            )""")
      cur.execute(sql)
      db.commit()

      # note that medrxiv have a unique DOI.
      # medrxiv has just a unique arxiv_id, that goes into the doi field!
      sql=("""create table preprint_articles (
            id int generated always as identity primary key,
            doi text,
            doc_id text not null unique,
            server text not null,
            url text not null,
            date date not null,
            title text not null,
            authors text not null,
            abstract text not null,
            nlp_lemma_text text[] not null,
            nlp_lemma_pos text[] not null,
            nlp_lemmatization_by text not null,
            nlp_lang float
            )""")
      cur.execute(sql)
      db.commit()
      
      print("tables created")

   except (Exception, psycopg2.DatabaseError) as error:
      print("exception: ", error)
   finally:
      if(db):
         cur.close()
         db.close()

def insert_new_ids(ids):
   try: # connect to PostgreSQL Database
        db = psycopg2.connect(host="localhost", database=PSQL_DBNAME, user=PSQL_USER, password=PSQL_PASS)
        cur = db.cursor()

        sql = """INSERT INTO new_ids (id) VALUES(%s);"""
        data=[]
        for i in ids:
           data.append((i,))
           
        cur.executemany(sql, data)
        db.commit()
#        args_str = ','.join(cur.mogrify("(%s)", x) for x in ids)
#        cur.execute("insert into new_ids values " + args_str)
        db.commit()

   except (Exception, psycopg2.DatabaseError) as error:
        print("exception: ", error, repr(error))
   finally:
        if(db):
            cur.close()
            db.close()


def insert_topics(topics):
    print(" db: adding topics", len(topics))
    try: # connect to PostgreSQL Database
        db = psycopg2.connect(host="localhost", database=PSQL_DBNAME, user=PSQL_USER, password=PSQL_PASS)
        cur = db.cursor()

        sql = """INSERT INTO topics (topic_id, label, generation_date, words, wordfreq) VALUES(%s, %s, %s, %s, %s)"""
        data=[]
        for t in topics:
           data.append((t['topic_id'], t['label'], t['generation_date'], t['words'], t['wordfreqs']))
        cur.executemany(sql, data)
        db.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("exception: ", error, repr(error))
    finally:
        if(db):
            cur.close()
            db.close()

def insert_topics_per_article(topics):
    print(" db: adding topics per article", len(topics))
    try: # connect to PostgreSQL Database
        db = psycopg2.connect(host="localhost", database=PSQL_DBNAME, user=PSQL_USER, password=PSQL_PASS)
        cur = db.cursor()

        sql = """INSERT INTO topics_per_article (article_id, topic_max, topic_ids, topics_weight, new) VALUES(%s, %s, %s, %s, %s)"""
        data=[]
        for t in topics:
           data.append((t['article_id'], t['topic_max'], t['topic_ids'], t['topics_weight'], t['new']))
        cur.executemany(sql, data)
        db.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("exception: ", error, repr(error))
    finally:
        if(db):
            cur.close()
            db.close()            






   

def insert_preprint(dict_articles, fill_all_articles_directly):
    print(" db: adding preprints", len(dict_articles))
    try: # connect to PostgreSQL Database
        db = psycopg2.connect(host="localhost", database=PSQL_DBNAME, user=PSQL_USER, password=PSQL_PASS)
        cur = db.cursor()

        sql = """INSERT INTO preprint_articles (doc_id, doi, server, url, date, title, authors, abstract, nlp_lemma_text, nlp_lemma_pos, nlp_lemmatization_by, nlp_lang) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        
        data=[]
        for art in dict_articles:
           data.append((art['doc_id'], art['doi'], art['server'], art['url'], art['date'], art['title'], art['authors'], art['abstract'], art['nlp_lemma_text'], art['nlp_lemma_pos'], art['nlp_lemmatization_by'], art['nlp_lang']))
           
        
        cur.executemany(sql, data)
        db.commit()

        if fill_all_articles_directly:
           # also fill directly into all_articles table

           data=[]
           for art in dict_articles:
              data.append((art['doc_id'], art['doi'], art['server'], art['url'], art['date'], art['title'], art['authors'], art['abstract'], art['nlp_lemma_text'], art['nlp_lemma_pos'], art['nlp_lemmatization_by'], art['nlp_lang'], art['server']))
           
           sql=("insert into all_articles (doc_id, doi, server, url, date, title, authors, abstract, nlp_lemma_text, nlp_lemma_pos, nlp_lemmatization_by, nlp_lang, journal) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
#           code.interact(banner="ppr2", readfunc=None, local=locals(), exitmsg=None)
           cur.executemany(sql,data)
           db.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("exception: ", error, repr(error))
    finally:
        if(db):
            cur.close()
            db.close()



         
def insert(dict_articles, fill_all_articles_directly):
    print(" db: adding", len(dict_articles))
    try: # connect to PostgreSQL Database
        db = psycopg2.connect(host="localhost", database=PSQL_DBNAME, user=PSQL_USER, password=PSQL_PASS)
        cur = db.cursor()
        
        data=[]
        for art in dict_articles:
           data.append((art['pubmed_id'], art['title'], art['abstract'], art['journal'], art['publication_date'], art['authors'], art['doi'], art['url'], "pubmed", art['recheck'], art['nlp_lemma_text'], art['nlp_lemma_pos'], art['nlp_lemmatization_by'], art['nlp_lang']))

        sql = """INSERT INTO pubmed_articles (pmid, title, abstract, journal, date, authors, doi, url, server, recheck, nlp_lemma_text, nlp_lemma_pos, nlp_lemmatization_by, nlp_lang) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        cur.executemany(sql,data)
        db.commit()

        if fill_all_articles_directly:
           # also fill directly into all_articles table
           data=[]
           for art in dict_articles:
              # skip if title=="" or recheck==1?
              if art['title']=="" or art['recheck']==1:
                 continue
              data.append((art['pubmed_id'], art['doi'], 'pubmed', art['url'], art['publication_date'], art['title'], art['authors'], art['abstract'], art['nlp_lemma_text'], art['nlp_lemma_pos'], art['nlp_lemmatization_by'], art['nlp_lang'], art['journal']))
          
           sql=("insert into all_articles (doc_id, doi, server, url, date, title, authors, abstract, nlp_lemma_text, nlp_lemma_pos, nlp_lemmatization_by, nlp_lang, journal) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
           cur.executemany(sql,data)
           db.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("exception: ", error, repr(error))
        print("in pmid:", art['pubmed_id'])
    finally:
        if(db):
            cur.close()
            db.close()



def query(query):
    try: # connect to PostgreSQL Database
        db = psycopg2.connect(host="localhost", database=PSQL_DBNAME, user=PSQL_USER, password=PSQL_PASS)
        cur = db.cursor()
        cur.execute(query)
        
        columns=[]
        for c in cur.description:
           columns.append(c[0])
        
        listdata=cur.fetchall() # gets list
        dictdata=[]
        for l in listdata:
           el={}
           for i in range(0,len(columns)):
              el[columns[i]]=l[i]
           dictdata.append(el)           
        
        return dictdata

    except (Exception, psycopg2.DatabaseError) as error:
        print("exception: ", error, repr(error))
    finally:
        if(db):
            cur.close()
            db.close()



   





# debug helpers
def insert_dupes(data):
    try: # connect to PostgreSQL Database
        db = psycopg2.connect(host="localhost", database=PSQL_DBNAME, user=PSQL_USER, password=PSQL_PASS)
        cur = db.cursor()

        sql = """INSERT INTO duplicates (title, info) VALUES(%s, %s);"""
        cur.execute(sql, data)
        db.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("exception: ", error, repr(error))
    finally:
        if(db):
            cur.close()
            db.close()



def insert_lang(data):
    try: # connect to PostgreSQL Database
        db = psycopg2.connect(host="localhost", database=PSQL_DBNAME, user=PSQL_USER, password=PSQL_PASS)
        cur = db.cursor()

        sql = """INSERT INTO lang_drops (text, drops, lang, score) VALUES(%s, %s, %s, %s);"""
        cur.execute(sql, data)
        db.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("exception: ", error, repr(error))
    finally:
        if(db):
            cur.close()
            db.close()
