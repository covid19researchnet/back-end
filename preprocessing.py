#!/usr/bin/env python

"""Returns lemmas and their respective part-of-speech from text.

Removes stopwords, punctuation and numbers.

"""


import spacy
from spacy_langdetect import LanguageDetector
import covdb
from helpers import *

sample = 'In the ongoing pandemic of coronavirus disease 2019 (COVID-19), the novel virus SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2) is infecting a naïve population. The innate immunity of the infected patient is unable to mount an effective defense, resulting in a severe illness with substantial morbidity and mortality. As most treatment modalities including antivirals and anti-inflammatory agents are mostly ineffective, an immunological approach is needed. The mechanism of innate immunity to this viral illness is not fully understood. Passive immunity becomes an important avenue for the management of these patients. In this article, the immune responses of COVID-19 patients are reviewed. As SARS-CoV-2 has many characteristics in common with two other viruses, SARS-CoV that cause severe acute respiratory syndrome (SARS) and MERS-CoV (Middle East respiratory syndrome coronavirus) that causes Middle East respiratory syndrome (MERS), the experiences learned from the use of passive immunity in treatment can be applied to COVID-19. The immune response includes the appearance of immunoglobulin M followed by immunoglobulin G and neutralizing antibodies. Convalescent plasma obtained from patients recovered from the illness with high titers of neutralizing antibodies was successful in treating many COVID-19 patients. The factors that determine responses as compared with those seen in SARS and MERS are also reviewed. As there are no approved vaccines against all three viruses, it remains a challenge in the ongoing development for an effective vaccine for COVID-19.'

langsample = "The novel coronavirus, also known as COVID-19, has moved rapidly across the world in 2020. This article reports on the recent consequences of the pandemic for early childhood education in Sweden, Norway, and the United States. The authors illustrate the effects of the pandemic on preschools in their countries, against a backdrop of frequent changes in infection and mortality rates, epidemiological understandings, government strategies, and mitigation strategies regarding preschool closures. Teachers report their experiences and actions in specific early childhood education settings, across the three national contexts. These experiential snapshots identify program priorities, parents' and children's reactions, and the commitment and concerns of teachers. The conversations reveal culturally situated similarities of early childhood educational practices but also differences across contexts. Teachers report on the challenges of their experiences but also benefits for their practice and how they engage with children and their families. Ideas about future preparedness for such pandemics are also discussed.Le nouveau coronavirus, également connu sous le nom de COVID-19, s’est déplacé rapidement à travers le monde en 2020. Cet article rend compte des conséquences récentes de la pandémie pour l’éducation de la petite enfance en Suède, en Norvège et aux États-Unis. Les auteurs analysent les effets de la pandémie sur les établissements préscolaires dans leurs pays, dans un contexte de changements fréquents des taux d’infection et de mortalité, de compréhension épidémiologique, de stratégies gouvernementales et de stratégies d’atténuation au regard des fermetures d’établissements préscolaires. Les enseignants font part de leurs expériences et de leurs actions dans des milieux spécifiques d’éducation de la petite enfance, dans les trois contextes nationaux. Ces instantanés expérientiels identifient les priorités du programme, les réactions des parents et des enfants, ainsi que l’engagement et les préoccupations des enseignants. Les conversations révèlent des similitudes culturelles des pratiques éducatives en éducation de la petite enfance, mais aussi des différences selon les contextes. Les enseignants rendent compte de défis de leurs expériences, mais aussi de bénéfices pour leur pratique et de la façon dont ils interagissent avec les enfants et leurs familles. Des idées sur la préparation future à de telles pandémies sont également discutées.El nuevo virus corona, conocido también como COVID-19, se ha movido rápidamente por todo el mundo en el 2020. Este artículo informa sobre las consecuencias de la pandemia sobre la educación temprana en Suecia, Noruega, y los Estados Unidos. Los autores muestran los efectos de la pandemia en los establecimientos preescolares en sus países en un contexto de cambios frecuentes de las tasas de infección y mortalidad, entendimientos epidemiológicos, estrategias gubernamentales, y estrategias de mitigación relacionadas con el cierre de los recintos preescolares. Los maestros reportan sus experiencias y acciones en ambientes específicos de la educación temprana, en los tres contextos nacionales. Estas impresiones instantáneas experimentales identifican las prioridades del programa, las reacciones de los padres y niños, y el compromiso y preocupaciones de los maestros. Las conversaciones revelan similitudes culturalmente situadas de las prácticas en la educación temprana, pero también revelan diferencias entre contextos. Los maestros reportan sobre los desafíos de sus experiencias, pero también sobre los beneficios de su práctica y cómo se relacionan con los niños y sus familias. También se discuten ideas sobre la preparación para futuras pandemias."

# choose language and initialize spacy model
spacy_model = "en_core_web_sm"
#nlp = spacy.load(spacy_model, disable = ['parser', 'ner'])
nlp = spacy.load(spacy_model, disable = ['ner'])
nlp.add_pipe(LanguageDetector(), name='language_detector', last=True)

# set some common words to remove
ignore = ['sars',
          'sars-cov-2',
          'covid',
          'coronavirus',
          'covid-19',
          'cov-2',
          'cov',
          'pandemic',
          'coronaviruse',
          'virus',
          '-pron-']

def preprocess(text):
    """Transform string to nested dict of POS and lemmas.    
    Parameters:
    text (str): text to lemmatize.
    Returns:
    lemmas, pos, language
    """    
    doc = nlp(text)
    text_en=""
    text_dropped="" # todo just for debug
    for sent in doc.sents:
        lan=sent._.language['language']
        prob=sent._.language['score']
        if lan=='en': # TODO doesnt always work. and gets us some empty nlp lang text
            # TODO may drop (random?) parts of abstract...
            text_en+=" "+sent.lower_
            if prob<LANG_WARNING_SCORE:
                if DEBUG:
                    print(" WARNING: sentence en lan prob<", LANG_WARNING_SCORE,"" ,prob, '"', sent.text, '"')
                if CREATE_LANG_DROPPED_TABLE:
                    covdb.insert_lang([text, sent.text, lan, prob])
        else:
            #text_dropped+=" "+sent.text
            if CREATE_LANG_DROPPED_TABLE:
                covdb.insert_lang([text, sent.text, lan, prob])

    doc=nlp(text_en)
    prob=doc._.language['score']
    lan=doc._.language['language']
    r={}
    r['nlp_lemma_text']=[token.lemma_.lower() for token in doc
                         if token.is_alpha and
                         not token.is_stop and
                         token.lower_ not in ignore]
    r['nlp_lemma_pos']=[token.pos_ for token in doc
                        if token.is_alpha and
                        not token.is_stop and
                        token.lower_ not in ignore]
    r['nlp_lang']=prob

    if prob<LANG_WARNING_SCORE:
        if DEBUG:
            print(" WARNING WHOLE DOC eng prob<90% for whole document:", prob)
            print('"', doc.text, '"')
        if CREATE_LANG_DROPPED_TABLE:
            covdb.insert_lang([text, doc.text, lan, prob])
        
    return r
