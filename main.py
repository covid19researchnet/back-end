#
# covid 19 research network
# written by Armin Pournaki and Alexander Dejaco
# in part funded by the European Open Science Cloud
#
# under GPL License
#

#!/usr/bin/env python

import os
import sys
import getopt

# own modules
from helpers import *
import covdb
from fetch_data import fetch_data
from topicmodel import renew_topic_model
from generate_graph import generate_graph
from generate_charts import generate_charts

def usage():
   print("usage: run.py [-hdg]\n")
   print("optional arguments:")
   print("  -h, --help    show this help message")
   print("  -y, --deploy  run deployment mode")
   print("  -d, --debug   run in debug mode (more debug messages)")
   print("  -c, --create-database:   deletes and recreates the database")
   print("  -f  --fetch-data:        only fetch new data")
   print("  -r  --renew-topic-model: recreates topic-model")
   print("  -g  --generate-graph:    only generate graphs")
   print("  -s  --generate-charts:    only generate charts")

def sel_fetch_data():
   fetch_data()

def sel_renew_topic_model():
   renew_topic_model()

def sel_generate_graphs():
   generate_graph()
   
   
switcher = {
   1: sel_fetch_data,
   2: sel_renew_topic_model,
   3: generate_graph
   }

# --------------------------------      -------------------------------
# -------------------------------- MAIN -------------------------------
# --------------------------------      -------------------------------
if __name__ == "__main__":
   run_selection=0
   deploy=0   

   try: # PROCESS ARGUMENTS
      opts, args = getopt.getopt(sys.argv[1:], "hycdfrgs", ["help", "deploy", "create-database", "debug", "fetch-data", "renew-topic-model", "generate-graph","generate-charts"])
   except getopt.GetoptError as err:
      print(str(err))
      usage()
      sys.exit(2)
      
   for o,a in opts:
      if o in ("-h", "--help"):
         usage()
         sys.exit()
      elif o in ("-y", "--deploy"):
         deploy=1
      elif o in ("-c", "--create-database"):
         DELETE_DB_AT_START=1 #variables not global.. todo not working
      elif o in ("-d", "--debug"):
         DEBUG=1
      elif o in ("-f", "--fetch-data"):
         run_selection=1
      elif o in ("-r", "--renew-topic-model"):
         run_selection=2
      elif o in ("-g", "--generate-graph"):
         run_selection=3
      elif o in ("-s", "--generate-charts"):
         run_selection=4         
      else:
         assert False, "unhandled option"

   if not run_selection:
      fetch_data()
      if DELETE_DB_AT_START:
         renew_topic_model()
      generate_graph()
      generate_charts()
   else:
      func=switcher.get(run_selection)
      func()
         
   cexit()

#   articles=get_articles() # articles table
#   nodes, links=language_processing()
#   colordict=create_graph()
#   plot_stats()
#   if deploy: deployment()
