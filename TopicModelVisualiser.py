def TopicModelVisualiser(lda_model, id2word, coherence_model_lda, corpus):
    
    import math
    from wordcloud import WordCloud
    import matplotlib.pyplot as plt
    import pandas as pd
    import numpy as np
    
    topicsdict = {"topics": []}

    # lda_model = inParams["model_object"]
    # id2word   = inParams["vocabulary_object"]
    # coherence_model_lda = inParams["coherence_object"]
    # corpus   = inParams["corpus_object"]

    n_topics = len(lda_model.get_topics())

    for i in range(n_topics):
        topic = lda_model.get_topic_terms(i)
        wordfreqdict = {}
        for wtuple in topic:
            wordidx = wtuple[0]
            word = id2word[wordidx]
            wordfreqdict[word] = wtuple[1]
        topicsdict["topics"].append(wordfreqdict)

    colorsmap = ['#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#46f0f0', '#f032e6', '#bcf60c', '#fabebe']

    if n_topics > 10:
        for i in range (math.ceil(n_topics/10)):
            colorsmap.extend(colorsmap)

    fig = plt.figure(figsize=(12,12))
    fig.suptitle(f"Wordclouds per topic", fontsize=20)

    mplindex = math.ceil(n_topics/4)

    for topic_idx in range(n_topics):

        wc = WordCloud(background_color=None, mode="RGBA", random_state=42, color_func=lambda *args, **kwargs: colorsmap[topic_idx])
        wc.fit_words(topicsdict["topics"][topic_idx])

        ax = fig.add_subplot(mplindex, 4, topic_idx+1)
        ax.set_title(f"Topic {topic_idx+1}")
        ax.imshow(wc, interpolation='bilinear')
        plt.axis('off')
        plt.tight_layout(rect=[0, 0.03, 1, 0.97])
    
    plt.close()
    coherence = coherence_model_lda.get_coherence_per_topic()
    df = pd.DataFrame({"Topic": np.arange(1, n_topics+1), "Coherence": coherence})
    fig2 = df.plot.bar(x="Topic", y="Coherence", figsize = (10,5), rot = 0, title="Coherence per Topic")
    df2 = pd.DataFrame({"Overall Coherence": [coherence_model_lda.get_coherence()], "Perplexity": [lda_model.log_perplexity(corpus)]})
    
    #plt.close()
    return fig,fig2,df,df2