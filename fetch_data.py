import json
import ast
import collections
import urllib.request
import numpy as np
from tqdm import tqdm
from pymed import PubMed
from pymed.helpers import batches
import datetime
import arxiv
import sys
import re
import itertools
import topicmodel

# debugging
import code

# own modules
import covdb
from preprocessing import preprocess
from helpers import *

def recheck(recheck_articles):
    print("running re-checks on", len(recheck_articles), "articles")
    recheckstring=""
    for article in recheck_articles:
        recheckstring+="," + str(article['pmid'])
    recheckstring+="[pmid]"

    print("requesting data from pubmed")
    assert(len(recheck_articles)<1000)
    results=pubmed_api.query(query=recheckstring, max_results=1000)
    print("data received")

    recheck_success=0
    recheck_len=0
    for article in results:
        update=0
        recheck_len+=1
        articleDict=article.toDict()

        if articleDict['publication_date']==None: # still no new data
            continue

        if type(articleDict['publication_date']) == datetime.date:
            update=1
        else:
            try: # normally its a year only
                y=int(articleDict['publication_date'])
                if y < 2030 and y > 1800:
                    update=1
                    if y==2020:
                        articleDict['publication_date'] = datetime.date.today() # for now set to NOW
                    else:
                        articleDict['publication_date'] = datetime.date(y, 1, 1) # for now set to 1.1.
                else:
                    print("unexpected date value ERROR IN RECHECK")
                    assert(0)
            except:
                print("WRONG DATE FORMAT", articleDict['pubmed_id'], articleDict['publication_date'])
                assert(0)

        if update: # TODO: also add them now to all_articles!
            recheck_success+=1
            covdb.cmd(f"update pubmed_articles set date='{articleDict['publication_date']}' where pmid={articleDict['pubmed_id']}")
            covdb.cmd(f"update pubmed_articles set recheck=0 where pmid={articleDict['pubmed_id']}")
            # TODO also change in all_articles? or dont add to all_articles when date is unknown
            
    print("found date in", recheck_success, "out of", recheck_len, " at recheck")

    assert(recheck_len==len(recheck_articles))





    
    
# ------------------------ ---- --------------------------
# ------------------------ MAIN --------------------------
# ------------------------ ---- --------------------------
def fetch_data():
    print("---")
    print("FETCH LATEST ARTICLE DATA")
    print("---")

    # --- PUBMED ---

    print("Connecting to PubMed API...")

    if DELETE_DB_AT_START:
        print("deleting DB:")
        print("ARE YOU SURE YOU WANT TO DELETE THE DATABASE? Press any key to continue... (otherwise ctrl+c to cancel)")        
        input()
        input()
        covdb.create() #just during debugging

    fill_all_articles_directly=not DELETE_DB_AT_START # if db is deleted, then dont fill directly

    if CREATE_LANG_DROPPED_TABLE:
        covdb.cmd("drop table if exists lang_drops")
        covdb.cmd("create table lang_drops (id int generated always as identity, text text not null, drops text not null, lang text not null, score float)")


    # check number of existing articles
    sql="select count(*) from pubmed_articles"
    n_existing=covdb.query(sql)[0]['count']

    # check how many new articles there are
    querystring = 'covid or covid-19' # TODO corona etc?
    pubmed_api = PubMed(tool="covid_literature", email="pournaki@mis.mpg.de")
    n_total = pubmed_api.getTotalResultsCount(query=querystring)
    n_new = n_total - n_existing
    print(n_existing, n_total, n_new)
    print(f"There are {n_new} more articles on PubMed compared against the local database.")

    # TODO check, this technique (n_new-n_existing) seems to be flawed. I don't know why.
    # But maybe we need to
    # check all new pmid's against all pmid's in the db. And add only the new ones.?

    # recheck data of articles in DB with recheck flag (mostly published ahead of print with missing date)
    recheck_articles=covdb.query("select pmid from pubmed_articles where recheck=1")
    print(f"We have {len(recheck_articles)} old articles in pubmed that need rechecking!")
    if len(recheck_articles)>0:
        recheck(recheck_articles)


    # retrieve new articles
    pubmedList = []

    # get ALL article id's
    article_ids_total=pubmed_api._getArticleIds(querystring, max_results=n_total)

    # CHECKING FOR PRE-EXISTING ARTICLES IN DATABASE
    print("creating list of pmids missing in the database:")

    #    teststr="select pmid from pubmed_articles where "
    #    for i in tqdm(article_ids_total):
    #        teststr+="pmid!="+str(i)+" and "
    #    teststr=teststr[:-4]

        # TODO maybe faster to find new pmids:
        # add list of ALL pmids to db new_ids
        # then find all expect intersection between pmid and new_ids ?
    covdb.cmd("drop table if exists new_ids")
    covdb.cmd("create table new_ids (id int primary key)")
    covdb.insert_new_ids(article_ids_total)
    new_ids=covdb.query("select id from new_ids except select pmid from pubmed_articles;")
    covdb.cmd("drop table if exists new_ids")
    new_ids_list=[]
    for n in new_ids:
        new_ids_list.append(n['id'])

    if DEBUG_WITH_SMALL_DATASET:
        new_ids_list=new_ids_list[0:200]

    # TODO sanity check select from pubmed except new_ids NEEDS TO BE EMPTY!? (but its not :p)
    print("done determinining new pmids via sql:", len(new_ids_list), "new articles")

    # GET NEW PUBMED ARTICLES AND INSERT INTO DB
    if len(new_ids_list)>0:
        data = list(
            [
                pubmed_api._getArticles(article_ids=batch)
                for batch in batches(new_ids_list, 250)
            ]
        )
        results=itertools.chain.from_iterable(data)

        # we get: ['pubmed_id', 'title', 'abstract', 'keywords', 'journal', 'publication_date', 'authors', 'methods', 'conclusions', 'results', 'copyrights', 'doi', 'xml']    
        # some have different keys (books?):
        # dict_keys(['pubmed_id', 'title', 'abstract', 'publication_date', 'authors', 'copyrights', 'doi', 'isbn', 'language', 'publication_type', 'sections', 'publisher', 'publisher_location'])

        print("parsing pubmed data...")
        for article in tqdm(results):
            articleDict = article.toDict()

            articleDict['recheck']=0
            # -------------------- clean data ------------------
            # PUBMED-ID
            # sometimes we get a contatenated string of pubmedids.. strange!
            if '\n' in articleDict['pubmed_id']:
                articleDict['pubmed_id'] = article.pubmed_id.split()[0] # use first pmid, discard others

            # TITLE
            # clean brackets out of title
            articleDict["title"] = articleDict["title"].replace("[", "").replace("]", "")

            # TODO for now keep them in DB, we can easily handle that with sql query...
            # TODO it seems ALL of the articles with empty title, HAVE a title, but
            #      the api (or the python implementation?) does not return it!! todo.. mayb WOS?
            # skip articles with empty titles
            #if articleDict['title'] == "":
            #    continue

            # AUTHORS
            authors = ""
            for author in articleDict["authors"]:
                ln = author["lastname"]
                fn = author["firstname"]
                try:
                    a = fn + " " + ln
                except TypeError:
                    a = ""
                authors += a
                authors += ", "
            authors = authors [:-4]
            articleDict["authors"] = authors

            # ABSTRACTS
            # handle empty abstracts
            if articleDict["abstract"] == None or articleDict["abstract"] == "":
                articleDict["abstract"] = ""
            else:
                articleDict["abstract"] = articleDict["abstract"].replace("\n", "")        

            # KEYWORDS
            try:
                if articleDict["keywords"] == None or \
                   articleDict["keywords"] == "" or \
                   articleDict["keywords"] == []:
                    articleDict["keywords"] = ""
            except KeyError:
                articleDict["keywords"] = ""

            # JOURNAL/PUBLISHER
            # when there is no journal its probably an online letter or something with publisher field
            # todo make a note in database that its not a journal article?
            if not 'journal' in articleDict.keys():
                assert('publisher' in articleDict.keys())
                articleDict['journal']=articleDict['publisher'] # TODO earlier version set journal to NONE

            # URL
            articleDict['url']=f"https://pubmed.ncbi.nlm.nih.gov/{articleDict['pubmed_id']}/"


            # DATE
            if articleDict['publication_date'] == None:
                articleDict['recheck']=1             # add as "recheck later"
                articleDict['publication_date'] = datetime.date.today() # temporary!
                # probably a publish online ahead of print without date.
                print(" recheck flag for date of article", articleDict['pubmed_id'])
            else:
                if type(articleDict['publication_date']) != datetime.date: # TODO maybe also recheck those?
                    try: # normally its a year only
                        y=int(articleDict['publication_date'])
                        if y < 2030 and y > 1800:
                            if y==2020:
                                articleDict['publication_date'] = datetime.date.today() # for now set to NOW
                            else:
                                articleDict['publication_date'] = datetime.date(y, 1, 1) # for now set to 1.1.
                        else:
                            print("unexpected date value")
                            assert(0)
                    except:
                        print("WRONG DATE FORMAT", articleDict['pubmed_id'], articleDict['publication_date'])
                        assert(0)                

            # NLP
            r={'nlp_lemma_text':'{}', 'nlp_lemma_pos': '{}', 'nlp_lang': '-1', 'nlp_lemmatization_by': 'none'}
            if articleDict['abstract']!='' and len(articleDict['abstract'])>10:
                r=preprocess(articleDict['abstract'])
                r['nlp_lemmatization_by']='abstract'
            else:
                if articleDict['title']!='':
                    r=preprocess(articleDict['title'])
                    r['nlp_lemmatization_by']='title'
                    
            articleDict['nlp_lemma_text']=r['nlp_lemma_text']
            articleDict['nlp_lemma_pos']=r['nlp_lemma_pos']
            articleDict['nlp_lemmatization_by']=r['nlp_lemmatization_by']
            articleDict['nlp_lang']=r['nlp_lang']


            pubmedList.append(articleDict)

        if len(pubmedList) != len(new_ids_list):
            print("WARNING: asked for", len(new_ids_list), "articles, but rx", len(pubmedList))
            prompt()
            # this is probably a problem. don't know why it happens

        # TODO: publication_date, doi, abstract can come as "null" from api
        # nice is - sql auto enforces that pmid are unique, which is good

        # ADDING DATA TO DB. pubmed_articles and all_articles
        if len(pubmedList) > 0:
            print("adding pubmed data:")

            covdb.insert(pubmedList, fill_all_articles_directly)
            print(" db: inserted", len(pubmedList), "/", n_new, "into database")
            if fill_all_articles_directly: # means now its already in all_articles
                topicmodel.calc_topics_per_article(pubmedList, 'pubmed') # fill topics_per_articles table

    # get total pubmed article data from sql:
    sql="select * from pubmed_articles;"
    pml=covdb.query(sql) #pubmedlist_total
    pml_notitle_count=covdb.query("select count(*) from pubmed_articles where title=''")[0]['count']
    pml_notitle_noabstract_count=covdb.query("select count(*) from pubmed_articles where title='' and abstract=''")[0]['count']
    print(f"There are now {len(pml)} articles in the database. {pml_notitle_count} with no title, {pml_notitle_noabstract_count} with neither title or abstract")

    num_recheck=covdb.query('select count(*) from pubmed_articles where recheck=1')[0]['count']
    print(f"We have {num_recheck} new articles in pubmed that need rechecking later!")

    # ----------------------------------------------------------------------------------------------

    pml_noabstracts_count = covdb.query("select count(*) from pubmed_articles where abstract=''")[0]['count']
    pml_abstracts_count = covdb.query("select count(*) from pubmed_articles where abstract!=''")[0]['count']
    print("pubmed no abstract in ", pml_noabstracts_count, "/", len(pml))

    assert((pml_noabstracts_count+pml_abstracts_count)==len(pml))

    n_all = len(pml)
    percentage = int(np.round(pml_abstracts_count / n_all, 2) * 100)
    print(f"pubmed ~{percentage}% of articles have abstracts. {pml_notitle_noabstract_count} of those have no title")

    print("done updating pubmed")

    if len(pml) > n_total:
        print("WARNING: we have more articles than pubmed! pubmed has removed some?")
        prompt()
        # todo might need to implement a removal of deprecated articles at update!

    # ----------------------------------------------------------------------------------------------
    # --- MEDRXIV AND BIORXIV ---
    print("---")

    # TODO always recreate database, or check to add only new ones?
    # TODO NOW WITH 8000 articles here we cant do this!
    # let's assume that we get them sorted (Should be ok) - newest first
    # let's use DOI as identifier in the database as check that we add the correct new ones

    if BIORXIV_ONLINE:
        print("Connecting to medRxiv API...")
        #url = 'https://connect.medrxiv.org/relate/collection_json.php?grp=181'

        url = f'https://api.biorxiv.org/covid19/0'
        response = urllib.request.urlopen(url)
        data = response.read()       # a `bytes` object
        dataJson = json.loads(data.decode('utf-8'))

        mxv_total=dataJson['messages'][0]['total']
        mxv_count_per_batch=dataJson['messages'][0]['count']
        assert(len(dataJson['collection'])==mxv_count_per_batch)
        assert(dataJson['messages'][0]['status']=='ok')


        print(f"There are now {mxv_total} articles on medRxiv/bioRxiv.")
        sql="select count(*) from preprint_articles where server='medRxiv' or server='bioRxiv'"
        n_existing=covdb.query(sql)[0]['count']
        print(f"There are {n_existing} preprint_articles in local medRxiv/bioRxiv database")
        n_new = mxv_total-n_existing
        print(f"There are {n_new} new medrxiv articles since last update.")

        # UPDATE NOW WORKING
        # cursor 0 has "most recent" 30 papers.
        # but they are sorted by date apparently...
        # so not item 0 is the newest!! but somewhere in the list.....
        # probably on page 0, but we cant be sure
        #
        if DEBUG_WITH_SMALL_DATASET:
            mxv_total=200
        mxv_collection=[]
        if n_existing==0: # completely new db, load all pages
            print("loading all medrxiv articles to database...")

            for idx in tqdm(range(0, mxv_total, mxv_count_per_batch)):
                url = f'https://api.biorxiv.org/covid19/{idx}' # idx acts as fetch cursor
                response = urllib.request.urlopen(url)
                data = response.read()
                dataJson = json.loads(data.decode('utf-8'))
                assert(dataJson['messages'][0]['status']=='ok')
                mxv_collection+=dataJson['collection']

            mxv_collection.reverse() # old ones with lower id
        else: # we need only to find the new ones TODO skip if n_new=0?
            if n_new>0:
                print("finding only the new medrxiv articles...")

                new_found=0
                for idx in range(0, mxv_total, mxv_count_per_batch):
                    url = f'https://api.biorxiv.org/covid19/{idx}'
                    response = urllib.request.urlopen(url)
                    data = response.read()
                    dataJson = json.loads(data.decode('utf-8'))
                    assert(dataJson['messages'][0]['status']=='ok')

                    # todo check all articles on this page (newest by date)
                    # we need to find n_new ones that are not in our db yet
                    for el in dataJson['collection']:
                        if len(covdb.query(f"select id from preprint_articles where doi='{el['rel_doi']}'")) == 0: # doi in current db not found, therefore it's new
                            new_found+=1
                            mxv_collection.append(el)
                            #print(" new article found at cursor", idx, new_found, "doi:", el['rel_doi'])
                            if new_found>1:
                                if last_el==el:                        
                                    print("DEBUG")
                                    input()
                            last_el=el

                        if new_found==n_new:
                            break

                    if new_found==n_new:
                        print(" found all new articles! last at idx", idx)
                        break

                assert(new_found==n_new)

                mxv_collection.reverse() # then new articles get the higher id

                assert(len(mxv_collection)+n_existing==mxv_total) # this asserts also that total doesnt change while running

        print("processing medRxiv data...")
        if len(mxv_collection) > 0: # either new db or new articles to add
            medrxivList = []
            for el in tqdm(mxv_collection):
                authors = ""
                if el['rel_authors'] != None:
                    for author in el['rel_authors']:
                        authors += author['author_name']
                        authors += ", "
                    authors = authors[:-2] # removes last comma
                DOI = el['rel_doi'].replace('\\', '')
                abstract = el['rel_abs'].replace("\\", "")
                if abstract==None or len(abstract)<10:
                    abstract=''

                # NLP
                r={'nlp_lemma_text':'{}',
                   'nlp_lemma_pos': '{}',
                   'nlp_lang': '-1',
                   'nlp_lemmatization_by': 'none'}            
                if abstract!='' and len(abstract)>10:
                    r=preprocess(abstract)
                    r['nlp_lemmatization_by']='abstract'
                else:
                    if el['rel_title']!='':
                        r=preprocess(el['rel_title'])
                        r['nlp_lemmatization_by']='title'

                art = {'doi': DOI,
                       'doc_id': DOI,
                       'server': el['rel_site'],
                       'url': el['rel_link'].replace("\\", ""),           
                       'date': el['rel_date'],
                       'title': el['rel_title'],
                       'authors': authors,
                       'abstract': abstract,
                       'nlp_lemma_text': r['nlp_lemma_text'],
                       'nlp_lemma_pos': r['nlp_lemma_pos'],
                       'nlp_lang': r['nlp_lang'],
                       'nlp_lemmatization_by': r['nlp_lemmatization_by']
                }
                assert(el['rel_site'] in ['bioRxiv','medRxiv'])        
                medrxivList.append(art)

            covdb.insert_preprint(medrxivList, fill_all_articles_directly)
            if fill_all_articles_directly: # means now its already in all_articles
                topicmodel.calc_topics_per_article(medrxivList, 'medrxiv') # fill topics_per_articles table



    # --------- ARXIV -------------
#    covdb.cmd("delete from preprint_articles where server='arxiv'")

    print("---")
    print("Connecting to arXiv API... (this can take a while)")

    # http://export.arxiv.org/api/query?search_query=ti:%27COVID%27%20OR%20ti:%27coronavirus%27%20OR%20abs:%27COVID%27%20OR%20abs:%27coronavirus%27&start=0&max_results=10
    #arxiv_query = "ti:'COVID-19' OR ti:'SARS-CoV-2' OR ti:'coronavirus' OR abs:'COVID-19' OR abs:'SARS-CoV-2' OR abs:'coronavirus'"
    #arxiv_queries=["ti:'COVID' AND ti:'19'", "ti:'coronavirus' OR abs:'coronavirus'", "abs:'COVID' AND abs:'19'"]
    # TODO maybe make a nice helper function to use for medrxib and arxiv?

    # arxiv does not have DOI. so we use arvix id. they can get revisions, so id changes to
    #  ..v1 ..v2 ..v3 ..

    # TODO once to fix the db check all that have multiple v's (versions v1,v2,v3,...)
    #      and delete all but the newest


    
    
    max_results=5000
    if DEBUG_WITH_SMALL_DATASET:
        max_results=100
    arxiv_query = "ti:'COVID' OR ti:'coronavirus' OR abs:'COVID' OR abs:'coronavirus'"
    arxiv_search = arxiv.query(query=arxiv_query,
                               id_list=[],
                               max_results=max_results,
                               start = 0,
                               sort_by="submittedDate",
                               sort_order="descending",
                               iterative=False,
                               max_chunk_results=100)

    assert(len(arxiv_search)<5000) # otherwise we need to increase max_results TODO
    print(f"There are {len(arxiv_search)} articles on arXiv")

    arxivList = []
    for result in tqdm(arxiv_search):
        articleDict = {}
        articleDict["title"] = result["title"].replace("\n", "").replace("  ", " ")
        articleDict["date"] = result["published"][:10]
        articleDict["abstract"] = result["summary"].replace("\n", " ")
        authorlist = result["authors"]
        articleDict["authors"] = ", ".join(authorlist)
        articleDict["url"] = result["id"]
        if result["doi"] == "" or result["doi"] == None:
            result["doi"]="empty"
        articleDict['doc_id']=result['id'] # arxiv ID "http://arxiv.org/abs/yyyy.iiiiiv1"
        assert("http://arxiv.org/abs/" in result['id'])
        articleDict["doi"] = result["doi"] # many doi are empty here. so using arxiv id when necessary
        articleDict["server"] = "arxiv"
        arxivList.append(articleDict)

    n_total=len(arxivList)
    sql="select count(*) from preprint_articles where server='arxiv'"
    n_existing=covdb.query(sql)[0]['count']
    print(f"There are {n_existing} arxic articles in our database")
    n_new = n_total-n_existing
    print(f"There are {n_new} more arxiv articles vs our database.")

    if n_existing>0:
        print(" finding only new ones...")
        # check which are new, then only process and add those. TODO check what is faster:
        #  for pubmed we use sql exclude to get list of new ids. but then we need to O^2 anyways
        #  at medrxiv at the moment we go though all articles and check one by one with a query
        #  if article already exists. dont know what is faster, the single queries or a O^2 loop
        #  TODO maybe make one module/function for all servers, see existing branch (outdated) for that
        covdb.cmd("drop table if exists new_ids, new_ids_nopreexisting, new_ids_nopreexisting_werejustupdated, new_ids_nopreexisting_werejustupdated_oldid, new_ids_nopreexisting_werejustupdated_newid;")
        covdb.cmd("create table new_ids(id text primary key)")
        covdb.insert_new_ids([art['doc_id'] for art in arxivList])

        new_ids=covdb.query("select id from new_ids except select doc_id from preprint_articles where server='arxiv';") # ids, except clear duplicates

        print(f"There are {new_ids} unique new arxiv_ids vs our database (includes revisions)")
        
        # make new_ids_nopreexisting // just excludes clear mathes preexisting in preprint articles
        # still in clude version updated "dupes"
        covdb.cmd("select id into new_ids_nopreexisting from new_ids except select doc_id from preprint_articles where server='arxiv';")

        
        # of those find the ones that have just an update into new_ids_nopreexisting_werejustupdated
        # those are dupes, the older one needs to be updated in the db
        # just stores the substring, so need to find whole id in next step
        covdb.cmd("select substr(id,0,14+position('v' in substr(id,14,length(id)))-1) into new_ids_nopreexisting_werejustupdated from new_ids_nopreexisting intersect select substr(doc_id,0,14+position('v' in substr(doc_id,14,length(doc_id)))-1) from preprint_articles where server='arxiv';")

        
        # store NEW VERSION ID with whole id in new_ids_nopreexisting_werejustupdated_newid
        covdb.cmd("select id into new_ids_nopreexisting_werejustupdated_newid from new_ids_nopreexisting where substr(id,0,14+position('v' in substr(id,14,length(id)))-1) in (select substr from new_ids_nopreexisting_werejustupdated);")

        # store OLD VERSION ID in new_ids_nopreexisting_werejustupdated_oldid
        covdb.cmd("select doc_id into new_ids_nopreexisting_werejustupdated_oldid from preprint_articles where server='arxiv' and substr(doc_id,0,14+position('v' in substr(doc_id,14,length(doc_id)))-1) in (select substr from new_ids_nopreexisting_werejustupdated);")

        # just delete old versions, thats it?
        # then add whole new_ids_nopreexisting?
        
        # topics_per_article and topics db have dependency on all_articles with delete-on-cascade
        # need to delte from preprint_articles and all_articles
        covdb.cmd("delete from all_articles where doc_id in (select * from new_ids_nopreexisting_werejustupdated_oldid);")
        covdb.cmd("delete from preprint_articles where doc_id in (select * from new_ids_nopreexisting_werejustupdated_oldid);")

        n_updated=covdb.query("select count(*) from new_ids_nopreexisting_werejustupdated_newid")[0]['count']
        print(f"of those are old articles that were updated/revised: {n_updated}")

        if n_updated>0:
            arxiv_revised=covdb.query("select * from new_ids_nopreexisting_werejustupdated_newid")
            for arxid in arxiv_revised:
                print(arxid['id'])
    
        print(" processing...")
        
    arxivList_toadd=[]
    if len(arxivList) > 0:
        for articleDict in tqdm(arxivList): # TODO maybe core more efficiently. same module with medrxiv?
            if n_existing==0:
                add=1 # add all, since db empty
            else:
                add=0 # find new ones
                for i in new_ids: # is it in new_ids? then add
                    if articleDict['doc_id']==i['id']:
                        add=1
                        break                
            if add:
                # NLP
                r={'nlp_lemma_text':'{}', 'nlp_lemma_pos': '{}', 'nlp_lang': '-1', 'nlp_lemmatization_by': 'none'}
                if articleDict['abstract']!='' and len(articleDict['abstract'])>10:
                    r=preprocess(articleDict['abstract'])
                    r['nlp_lemmatization_by']='abstract'
                else:
                    if articleDict['title']!='':
                        r=preprocess(articleDict['title'])
                        r['nlp_lemmatization_by']='title'

                articleDict['nlp_lemma_text']=r['nlp_lemma_text']
                articleDict['nlp_lemma_pos']=r['nlp_lemma_pos']
                articleDict['nlp_lemmatization_by']=r['nlp_lemmatization_by']
                articleDict['nlp_lang']=r['nlp_lang']
                arxivList_toadd.append(articleDict)        

    if len(arxivList_toadd) > 0:
        covdb.insert_preprint(arxivList_toadd, fill_all_articles_directly)
        if fill_all_articles_directly: # means now its already in all_articles
            topicmodel.calc_topics_per_article(arxivList_toadd, 'arxiv') # fill topics_per_articles table

    sql="select count(*) from preprint_articles where server='arxiv'"
    n_existing=covdb.query(sql)[0]['count']
    print("done with arXiv")
    
    if n_new != len(arxivList_toadd):
        print(" WARNING should have", n_new, "new ones, but inserted ", len(arxivList_toadd))
        prompt()

    if n_total != n_existing:
        print(" WARNING should have total of", n_total, "but have", n_existing)
        prompt()


    # TODO are double articles possible? also or already exist in pubmed? YESSSSSSSSSSS
    # check new pubmed articles if they are listed in a prepint db already? (not at the moment as preprint db is recreated every time)


    # TODO there are empty lemmata. f.i. chinese abstract symbols, and some few with faulty/empty abstracts
    # most problems with titles and abstracts are probably due to special chars that the python lib cant handle


    # --------------------- MERGE to all_articles and create duplicates and notitle tables -----------

    # todo there might be doubles. union without 'all" gives less result!
    allarticles=covdb.query("select title from pubmed_articles union all select title from preprint_articles;")

    print("")
    if DELETE_DB_AT_START:
        print("merging all articles (with a title) into one table in db: all_articles")
        # recreates all_articles from scratch. only if db was cleared completely.
        covdb.merge() # without empty titles ! if abstract is empty - title is used for topic model

    # TODO check if all_articles are more than pubmed+preprint? (preprint might drop double DOI's !, all_articles does not!)





    # ---------DUPLICATES
    # TODO there are more with the same title. they are often not real duplicates!
    # most duplicates are either empty titles, or letters about articles, etc...
    # there are doubles etween preprints and pubmed!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    # check pubmed articles. pmid is unique. but now check titles:
    alltitles=[d['title'] for d in pml]
    dupes = [item for item, count in collections.Counter(alltitles).items() if count > 1]
    print("duplicate titles in pubmed_articles:", len(dupes))

    # check duplicates in preprints
    preprints=covdb.query("select title from preprint_articles;")
    alltitles=[d['title'] for d in preprints]
    dupes = [item for item, count in collections.Counter(alltitles).items() if count > 1]
    print("duplicate titles in preprint_articles:", len(dupes), "TODO CHECK!")
    #assert(len(dupes)==0)

    # check all together
    alltitles=[d['title'] for d in allarticles]
    dupes = [item for item, count in collections.Counter(alltitles).items() if count > 1]
    print("duplicate titles in all_articles:", len(dupes))

    if CREATE_DUPES_TABLE: #duplicates
        print("creating duplicates table in db...")
        covdb.cmd("drop table if exists duplicates")
        covdb.cmd("create table duplicates (id int generated always as identity, title text, info text not null)")
        for d in tqdm(dupes):
            d1=d.replace("'", "''")
    #        if d=="":
    #            res=json.dumps(covdb.query(f''' select doi,server from pubmed_articles where title ~* '{d1}' '''))
    #        else:

            res=json.dumps(covdb.query(f''' select doi,server from all_articles where title ~* '{d1}' '''))
            if res == None or res == []:
                print("d1", d1)
                prompt()
            covdb.insert_dupes([d, res])
        print("done duplicates table")

    # -------------EMPTY TITLES:
    # NOTE - most empty titles are probably just a problem with the API, as on pubmed they have titles. we need to handle them (maybe use web of science?)

    if DEBUG: #empty titles table
        covdb.cmd("drop table if exists emptytitles")
        covdb.cmd("create table emptytitles (id int generated always as identity, pmid int not null)")
        covdb.cmd("insert into emptytitles (pmid) select pmid from pubmed_articles where title=''")

    allarticles_noemptytitles=covdb.query("select title from pubmed_articles where title !='' union all select title from preprint_articles;")
    alltitles=[d['title'] for d in allarticles_noemptytitles]
    dupes = [item for item, count in collections.Counter(alltitles).items() if count > 1]

    print("duplicate titles except empty titles:", len(dupes))
    print("NUMBER OF EMPTY-TITLE PUBMED ARTICLES:", len(allarticles)-len(allarticles_noemptytitles))


    # TODO get the latest 1500
    # how to find out which are the latest?
    # use an id in the db?
    # use date?

    # using date:
    #pml_subset=covdb.query("select * from pubmed_articles order by date desc limit 1000")
    #preprint_subset=covdb.query("select * from preprint_articles order by date desc limit 500") # 500 all or 400/100 separately? TODO

    # or should we just use the 1500 latest, regardless where they come from?
#    latest_subset=covdb.query("select * from all_articles order by date desc limit 1500")

    #subset = pml[:1000] + medrxivList[:400] + arxivList[:100]
    # save that subset

    print("done")

