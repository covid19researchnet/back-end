# COVID-19 Research Network Back-End

Fetches COVID-19 related articles from PubMed, medRxiv, bioRxiv and arXiv to a PSQL-database, computes LDA-topic model, creates d3js-compatible semantic network of the latest 1500 articles. 


Prerequisites:
* Python 3
* Postgresql Database
* psycopg2>=2.8.5
* arxiv>=0.5.3
* pymed>=0.8.9
* spacy>=2.3.2
* wordcloud>=1.8.0
* scikit-learn>=0.23.2

Configuration:
* Configure psql database access in helpers.py

Run:
* python3 main.py --help
*   --create-database: deletes and recreates the psql database
*   --fetch-data: fetches new data from medline biorxiv and arxiv
*   --renew-topic-model: renews or creates the topic-model
