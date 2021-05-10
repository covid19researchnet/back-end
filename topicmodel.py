#
# covid 19 research network
# written by Armin Pournaki and Alexander Dejaco
# in part funded by the European Open Science Cloud
#
# under GPL License
#

import json
import numpy as np
from tqdm import tqdm
import pandas as pd
import gensim
import gensim.corpora as corpora
from gensim.models import CoherenceModel
import matplotlib.pyplot as plt
from TopicModelVisualiser import TopicModelVisualiser
import datetime

# own modules
import covdb
from helpers import *

# debugging
import code

# TODO why does make_brigrams_other always use Phrases from abstracts only (without titles)?
# compute bigrams 
def make_bigrams_own(texts):
    bigram = gensim.models.Phrases(texts, min_count=2, threshold=20) #
    bigram_mod = gensim.models.phrases.Phraser(bigram)
    return [bigram_mod[doc] for doc in texts]

def make_bigrams_other(texts,texts2):
    bigram = gensim.models.Phrases(texts, min_count=2, threshold=20) #
    bigram_mod = gensim.models.phrases.Phraser(bigram)
    return [bigram_mod[doc] for doc in texts2]

# only called after initial database creation (so only when fill_all_articles_directly==1)
# this uses the EXISTING lda model
# initial db creation does this with full topicmodel.py
# after that all_articles and topics_per_articles is directly filled
def calc_topics_per_article(data, server):
    
    # data is [] from fetch_data can be pubmedList, medrxivList, arxivList
    # need to get id from all_articles:
    # data['server'] should tell us which it is
    # for pubmed all_articles['doc_id']==pubmedArticle['pmid']
    # for medrxi all_articles['doc_id']==medrxiArticle['doc_id'] (its DOI)
    # for arrxiv all_articles['doc_id']== arxivArticle['doc_id'] (its arxiv_id 'id')

    print("calculating topic data for new articles", len(data))
    # data['id']
    # data['nlp_lemma_text']       confirm its not '{}'
    # data['nlp_lemmatization_by'] confirm its 'title' or 'abstract'
    id2word = gensim.corpora.Dictionary.load(LDA_DICT_FILE)
    lda_model = gensim.models.LdaModel.load(LDA_MODEL_FILE)

    # get ALL! preprocessed texts from all_articles
    abstracts_preprocessed=[n['nlp_lemma_text'] for n in covdb.query("select nlp_lemma_text from all_articles")]

    # get only NEW articles from all_articles
    articles=[]
    for d in data:        
        id_key='doc_id'
        if server=='pubmed':
            id_key='pubmed_id'
            
        # load it from all_articles
        article=covdb.query(f'''select id, nlp_lemma_text, nlp_lemmatization_by from all_articles where doc_id='{d[id_key]}' and nlp_lemma_text!='{{}}' ''')

        if len(article) != 1:
            print(" WARNING: nlp_lemma_text probably empty. skipped due to language during preprocessing?")
            continue
            
        assert(len(article)==1)
        articles.append(article[0])

    if len(articles) < len(data):
        print(" WARNING: adding less articles to topics_per_articles than new ones (empty abstract and title?)", "adding:", len(articles), "new-ones:", len(data))
        prompt()

    subset_preprocessed=[a['nlp_lemma_text'] for a in articles]
    
    # make bigrams of SUBSET (related to abstracts. TODO why not related to all texts?)
    subset_preprocessed_bigrams=make_bigrams_other(abstracts_preprocessed, subset_preprocessed)

    # get subset_corpus: out of preprocessed_bigrams
    subset_corpus = [id2word.doc2bow(text) for text in subset_preprocessed_bigrams]
    # get subset_topics: list of lda_model.get_document_topics of subset_corpus
    subset_topics = list(lda_model.get_document_topics(subset_corpus))

    article_topics=[]
    # calc topics from this data (subset_topics)
    for idx, distribution in enumerate(subset_topics):
        article_topic={}    
        x = np.array(distribution)
        probs = x[:,1]
        max_idx = np.argmax(probs)
        max_topic = x[max_idx][0]
        tidxlist = x[:,0].astype(int).astype(str)
        problist = probs.round(4)
        
        article_topic['article_id']=articles[idx]['id']
        article_topic['topic_max']=int(max_topic)
        article_topic['topic_ids']=[int(t) for t in tidxlist]
        article_topic['topics_weight']=[float(t) for t in problist]
        article_topic['new']=1
        article_topics.append(article_topic)

    covdb.insert_topics_per_article(article_topics)

    # to insert i need: article_id topic_max topic_ids[] topics_weight[] new
    # create data for insertion, add new=1!


    

# THIS DELETES THE topics_per_article database and recreates it!
# also updates the lda model over the whole database
def renew_topic_model():
    # articles that have abstract
    abstracts_data=covdb.query("select id,nlp_lemma_text from all_articles where nlp_lemma_text!='{}' and nlp_lemmatization_by='abstract'")
    abstracts_preprocessed = [d['nlp_lemma_text'] for d in abstracts_data]
    abstracts_ids = [d['id'] for d in abstracts_data]

    # only for articles without abstract
    titles_data=covdb.query("select id,nlp_lemma_text from all_articles where nlp_lemma_text!='{}' and nlp_lemmatization_by='title'")
    titles_preprocessed = [d['nlp_lemma_text'] for d in titles_data]
    titles_ids = [d['id'] for d in titles_data]

    print("calculating bigrams of abstracts...")
    abstracts_preprocessed_bigrams = make_bigrams_own(abstracts_preprocessed)

    print("calculating bigrams of titles...")
    titles_preprocessed_bigrams = make_bigrams_other(abstracts_preprocessed, titles_preprocessed)

    # create dictionary from the abstract
    id2word = corpora.Dictionary(abstracts_preprocessed_bigrams)

    # build corpus from above dictionary
    corpus_abstracts = [id2word.doc2bow(text) for text in abstracts_preprocessed_bigrams]
    corpus_titles = [id2word.doc2bow(title) for title in titles_preprocessed_bigrams]

    print("building gensis model...")
    lda_model = gensim.models.ldamulticore.LdaMulticore(corpus=corpus_abstracts, id2word=id2word, num_topics=10, workers=4, random_state=100, chunksize=1000, passes=12, per_word_topics=True)

    #lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus_abstracts, id2word=id2word, num_topics=12, random_state=100, update_every=1, chunksize=1000, passes=10, alpha='auto', per_word_topics=True)

    print("building coherence model...")
    coherence_model_lda = CoherenceModel(model=lda_model, texts=abstracts_preprocessed_bigrams, dictionary=id2word, coherence='u_mass')

    print("printing figure...")
    wordcloud, coherencebars, per_topic_coherence, coherence_perplexity = TopicModelVisualiser(lda_model, id2word, coherence_model_lda, corpus_abstracts)
    wordcloud.savefig("wordcloud.png")

    # TODO this is only abstracts
    # distribution of topics across ALL abstracts
    abstract_topics = list(lda_model.get_document_topics(corpus_abstracts)) # ABSTRACT TOPICS

    # TODO this is mixed titles/abstracts
    # only latest 1500 subset
    subset=covdb.query("select id,nlp_lemma_text,nlp_lemmatization_by from all_articles where nlp_lemmatization_by='abstract' or nlp_lemmatization_by='title' and nlp_lemma_text!='{}' order by date desc limit 1500")

    subset_preprocessed=[d['nlp_lemma_text'] for d in subset]
    subset_ids=[d['id'] for d in subset]    

    subset_preprocessed_bigrams = make_bigrams_other(abstracts_preprocessed,
                                                     subset_preprocessed)

    subset_corpus = [id2word.doc2bow(text) for text in subset_preprocessed_bigrams]
    subset_topics = list(lda_model.get_document_topics(subset_corpus)) # SUBSET TOPICS latest (titles+abstracts)

    titles_topics   = list(lda_model.get_document_topics(corpus_titles)) # TITLES TOPICS

    TOPICSdict = {}

    TOPICSdict["topics"] = {}
    TOPICSdict["article_topics"] = {}

    n_topics = len(lda_model.get_topics())
    for i in range(n_topics):
        topic = lda_model.get_topic_terms(i, topn=20)
        wordfreqdict = {}
        for wtuple in topic:
            wordidx = wtuple[0]
            word = id2word[wordidx]
            wordfreqdict[word] = np.round(float(wtuple[1]), 4)

        TOPICSdict["topics"][i] = {}
        TOPICSdict["topics"][i]["label"] = "Not yet"
        TOPICSdict["topics"][i]["wordfreqs"] = wordfreqdict

    for idx, distribution in enumerate(abstract_topics): # ABSTRACTS
        article_id = abstracts_ids[idx]
        x = np.array(distribution)
        probs = x[:,1]
        max_idx = np.argmax(probs)
        max_topic = x[max_idx][0]
        TOPICSdict["article_topics"][article_id] = {}
        TOPICSdict["article_topics"][article_id]["max"] = int(max_topic)

        tidxlist = x[:,0].astype(int).astype(str)
        problist = probs.round(4)
        distrib  = dict(zip(tidxlist, problist))
        TOPICSdict["article_topics"][article_id]["dist"] = distrib

    for idx, distribution in enumerate(titles_topics): # TITLES
        article_id = titles_ids[idx]
        x = np.array(distribution)
        probs = x[:,1]
        max_idx = np.argmax(probs)
        max_topic = x[max_idx][0]
        TOPICSdict["article_topics"][article_id] = {}
        TOPICSdict["article_topics"][article_id]["max"] = int(max_topic)

        tidxlist = x[:,0].astype(int).astype(str)
        problist = probs.round(4)
        distrib  = dict(zip(tidxlist, problist))
        TOPICSdict["article_topics"][article_id]["dist"] = distrib

    covdb.cmd("drop table if exists topics_per_article")
    covdb.cmd("drop table if exists topics") # TODO would incremental be possible? probably no, model has to be renewed when new articles join?
    covdb.cmd("create table topics (topic_id int primary key, label text not null, generation_date date not null, words text[] not null, wordfreq float[] not null)")
    covdb.cmd("create table topics_per_article (article_id int primary key references all_articles(id) on delete cascade, topic_max int not null references topics(topic_id) on delete cascade, topic_ids int[] not null, topics_weight float[] not null, new int not null)")

    n_topics = len(lda_model.get_topics())
    topics=[]
    for i in range(n_topics):
        topic={}
        t=TOPICSdict['topics'][i]
        topic['topic_id']=i
        topic['generation_date']=datetime.date.today()
        topic['label']=t['label']
        topic['words']=[w for w in t['wordfreqs']]
        topic['wordfreqs']=[t['wordfreqs'][w] for w in t['wordfreqs']]
        topics.append(topic)
    covdb.insert_topics(topics)

    article_topics=[]
    for a in TOPICSdict['article_topics']:
        at=TOPICSdict['article_topics'][a]
        article_topic={}
        article_topic['article_id']=a
        article_topic['topic_max']=at['max']
        article_topic['topic_ids']=[int(t) for t in at['dist']]
        article_topic['topics_weight']=[at['dist'][str(t)] for t in at['dist']]
        article_topic['new']=0 # only for incremental additions via 'calc_topic_per_article(data)'
        article_topics.append(article_topic)
    covdb.insert_topics_per_article(article_topics)

    # TODO check why is len(TOPICSdict) < len(all_articles)?


    # query examples:

    # select t.* from topics_per_article t where t.article_id=(select a.id from all_articles a where a.id=1);

    # EXMAPLE1 : get topics data and article date for latest 1500 articles, based on publication date:
    # subset=covdb.query('select all_articles.date, topics_per_article.* from topics_per_article inner join all_articles on article_id=id order by all_articles.date desc limit 1500;')

    # todo new articles (get_topics) add to topics_per_articles with a flag _new_ to add new ones without making a new model. remake the model just at intervals
    
    

#    with open ("./data/TOPICS.json", 
#               "w", 
#               encoding='utf-8') as f:
#        json.dump(TOPICSdict, f, ensure_ascii=False)

    id2word.save(LDA_DICT_FILE)
    lda_model.save(LDA_MODEL_FILE)
    corpora.MmCorpus.serialize(LDA_CORPUS_FILE, corpus_abstracts)

