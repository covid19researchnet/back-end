#
# covid 19 research network
# written by Armin Pournaki and Alexander Dejaco
# in part funded by the European Open Science Cloud
#
# under GPL License
#

# import libraries
import json
import spacy
import numpy as np
from tqdm import tqdm
from datetime import datetime
from collections import Counter
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.spatial.distance import pdist, squareform

# own modules
import covdb
from helpers import *

# debugging
import code

def generate_graph():
    # define your subset of articles
    subset_pubmed = covdb.query("select * from all_articles where server='pubmed' order by date desc limit 1000;")
    subset_preprints = covdb.query("select * from all_articles where server='biorxviv' or server='arxiv' or server='medrxiv' order by date desc limit 500;")
    subset = subset_pubmed + subset_preprints

    print("Loading articles subset and topics...")
    corpus = []
    metadata = []

    dstr = str(datetime.today())[:10]
    for article in subset:
        abstract = article["abstract"]
        title = article["title"]
        if abstract != None:
            corpus.append(abstract)
        else:
            corpus.append(title)

    ignoreLEM = ['sars-cov-2',
                 'covid',
                 'coronavirus',
                 'covid-19',
                 'cov-2',
                 'cov',
                 'pandemic',
                 'coronaviruse']

    relevantPOS = ['PNOUN','NOUN']

    class LemmaTokenizer(object):
        def __init__(self):
            self.spacynlp = spacy.load('en_core_web_sm')
        def __call__(self, doc):
            nlpdoc = self.spacynlp(doc)
            tokens = []
            for token in nlpdoc:
                if not token.is_punct and \
                not token.is_stop and \
                token.pos_ in relevantPOS and \
                token.lemma_.lower() not in ignoreLEM:
                    tokens.append(token.lemma_.lower())
            return tokens

    print("Preprocessing and calculating tf-idf scores...")
    dataset = corpus
    vectorizer = TfidfVectorizer(analyzer='word', tokenizer=LemmaTokenizer())
    X = vectorizer.fit_transform(dataset)

    print("Computing cosine similarity matrix...")
    from sklearn.metrics.pairwise import cosine_similarity
    cosar = cosine_similarity(X, X)
    for i in range(len(cosar)):
        cosar[i,i] = 0

    # find threshold value for edge between two articles
    t = 0.180
    e = ((cosar > t).sum()) / 2
    print(f"Keeping the threshold at {t}, there are {e} links in the resulting network.")

    print("Creating adjacency matrix...")
    adjmat = cosar.copy()
    adjmat[adjmat < t] = 0

    unconnected_nodes = 0
    for i in range(len(adjmat)):
        x = len(adjmat[i].nonzero()[0])
        if x == 0:
            unconnected_nodes += 1
    print(f"There are {unconnected_nodes} unconnected nodes.")

    topic_labels = []
    with open ("topics.txt", "r", encoding="utf-8") as f:
        for line in f:
            topic_labels.append(line.replace("\n", ""))
            
    print("Creating node and link list...")
    nodes = []
    faulty_articles = []
    for article in (subset):
        idx = article["id"]
        topic_dict = covdb.query(f"select * from topics_per_article where article_id = {idx};")

        try:
            topicidx = topic_dict[0]['topic_max']
        except IndexError:
            faulty_articles.append(article)
            topicidx = 1
        topiclabel = topic_labels[topicidx]
        if article["server"] == "pubmed":
            p = False
        else:
            p = True
        nodedict = {'id': idx,
                    'title': article["title"],
                    'topic': topiclabel,
                    'preprint': p}
        nodes.append(nodedict)

    print(f"The following articles are faulty and have no topic information:")
    print(faulty_articles)

    # create linklist
    adjmat = cosar.copy()
    adjmat[adjmat < t] = 0
    adjmat = np.triu(adjmat, k=0)
    #print(f"The network will have {((adjmat > t).sum())} links.")

    metadata = subset

    # colormap = ['#e6194b',
    # '#3cb44b',
    # '#ffe119',
    # '#4363d8',
    # '#f58231',
    # '#911eb4',
    # '#46f0f0',
    # '#f032e6',
    # '#bcf60c',
    # '#fabebe',
    # '#008080',
    # '#e6beff',
    # '#e6194b',
    # '#3cb44b',
    # '#ffe119',
    # '#4363d8',
    # '#f58231',
    # '#911eb4',
    # '#46f0f0',
    # '#f032e6']

    # use d3js colormap
    colormap = [ "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf" ]

    new_colordict = {}
    for idx, label in enumerate(sorted(topic_labels)):
        new_colordict[label] = colormap[idx]

    links = []
    edges = ((int(e[0]), int(e[1])) for e in zip(*np.asarray(adjmat).nonzero()))
    for (u,v) in edges:
        edict = {"source": metadata[u]["id"],
                 "target": metadata[v]["id"]}
        links.append(edict)

    fields = []
    for node in nodes:
        fields.append(f"{node['topic']}")
    fieldcounts = dict(Counter(fields).most_common())

    fullstr = ""
    for field in fieldcounts:
        dec1 = f"""<li style="color: {new_colordict[field]}" id="{field}" class="legend-element">"""
        dec2 = f"""<a class="fieldlinks" href="javascript:void(0)" """
        dec3 = f"""onclick="highfield('{field}')">"""
        dec4 = f"""{field} ({fieldcounts[field]} articles)</a></li>"""
        fullstr += dec1+dec2+dec3+dec4
        fullstr += "\n"

    print("Saving graph and metadata to site subdir...")

    # create d3graph
    d3graph = {'graph': {'date': str(datetime.now())[:16]+" UTC",
                         'N_nodes': len(nodes),
                         'N_links': len(links),
                         't_links': t,
                         'colordict': new_colordict,
                         'legend': fullstr},
               'nodes': nodes,
               'links': links}

    #print(f"There are {len(links)} links in the co-occurence network.")

    # save graph and metadata for site
    with open(SITESUBDIR + "graph.json", 'w', encoding='utf-8') as f:
        json.dump(d3graph, f, ensure_ascii=False)

    graph_metadata = {}
    for article in (metadata):

        if article["server"] == "pubmed":
            is_preprint = False
        else:
            is_preprint = True

        journal = article["journal"]
        
        #if "journal" in article.keys():
        #    journal = article["journal"]
        #    is_preprint = False
        #else:
        #    journal = article["server"]
        #    is_preprint = True

        idx = article["id"]
        topic_dict = covdb.query(f"select * from topics_per_article where article_id = {idx};")
        try:
            topicidx = topic_dict[0]['topic_max']
        except IndexError:
            topicidx = 1
        topiclabel = topic_labels[topicidx]
        if article["abstract"] == None:
            abstract = article["title"]
        else:
            abstract = article["abstract"]

        graph_metadata[idx] = {
            'doi': article['doi'],
            'journal': journal,
            'url': article['url'],
            'date': str(article['date']),
            'title': article['title'],
            'subfield': topiclabel,
            'authors': article['authors'],
            'abstract': abstract,
            'preprint': is_preprint
        }
    with open(SITESUBDIR + "graph_metadata.json", 'w', encoding='utf-8') as f:
        json.dump(graph_metadata, f, ensure_ascii=False)

    print("DONE")
