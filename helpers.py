#
# covid 19 research network
# written by Armin Pournaki and Alexander Dejaco
# in part funded by the European Open Science Cloud
#
# under GPL License
#

import sys

# SITES ONLINE?
BIORXIV_ONLINE=0



# DEBUG FLAGS

DEBUG=0
DELETE_DB_AT_START=0
DEBUG_BREAKS=0
DEBUG_WITH_SMALL_DATASET=0

LANG_WARNING_SCORE=0.8

CREATE_LANG_DROPPED_TABLE=1
CREATE_DUPES_TABLE=0 # takes a few minutes

if not DEBUG:
    DEBUG_BREAKS=0
    LANG_WARNING_SCORE=0
    DELETE_DB_AT_START=0

# CONSTANTS
PSQL_DBNAME="foobase"
PSQL_USER="foouser"
PSQL_PASS="foopass"

# DIRECTORIES
LDA_DICT_FILE="./topicmodel/DICT.gensim"
LDA_MODEL_FILE="./topicmodel/MODEL.gensim"
LDA_CORPUS_FILE="./topicmodel/CORPUS.gensim"
SITESUBDIR = "./www/data/"

# HELPER FUNCTIONS
def prompt():
    if DEBUG_BREAKS:
        print("press key to continue...", end='')
        input()

def cexit():
    if DEBUG_BREAKS:
        print("press key to exit...", end='')
    sys.exit()
        
