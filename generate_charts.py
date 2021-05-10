#
# covid 19 research network
# written by Armin Pournaki and Alexander Dejaco
# in part funded by the European Open Science Cloud
#
# under GPL License
#

import numpy as np
from datetime import datetime
import pandas as pd

# own modules
import covdb
from helpers import *

def generate_charts():

    print("Generating stat charts...")
    subset = covdb.query("select * from all_articles inner join topics_per_article on id=article_id;")

    df = pd.DataFrame(subset)

    start = datetime.fromisoformat('2019-11-01 00:00:00')
    #end = datetime.fromisoformat('2021-03-31 23:59:59')

    df["date"] = df["date"].astype("datetime64")
    #df = df[ (df["date"].dt.to_pydatetime() >= start) & (df["date"].dt.to_pydatetime() <= end) ]
    df = df[ (df["date"].dt.to_pydatetime() >= start)]

    topic_labels = []
    with open ("topics.txt", "r", encoding="utf-8") as f:
        for line in f:
            topic_labels.append(line.replace("\n", ""))    
            
    def get_topic_from_index(topicidx):
        return topic_labels[topicidx]        

    df['topiclabel'] = df['topic_max'].apply(get_topic_from_index)

    for bywhat in ["server", "topiclabel"]:
        try:
            df = df.set_index("date")
        except KeyError:
            pass
        grouper = df.groupby([pd.Grouper(freq='1M'), bywhat])
        gdf = grouper[bywhat].count().unstack(bywhat).fillna(0)
        gdf["datetime"] = gdf.index    
        gdf.drop(columns=['datetime'], inplace=True)
        gdf.to_csv(SITESUBDIR + f"by_{bywhat}.csv")
        gdf.cumsum().to_csv(SITESUBDIR+ f"./by_{bywhat}_cs.csv")
