import time
import datetime
from collections import Counter, defaultdict, OrderedDict 
import xml.etree.ElementTree as ET
import argparse

import urllib.request
import urllib
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import os 
import pytz
import datetime as dt
import sys

OAI = "{http://www.openarchives.org/OAI/2.0/}"
ARXIV = "{http://arxiv.org/OAI/arXiv/}"


def harvest(arxiv, start_date, end_date):
    df = pd.DataFrame(columns=('arxiv_id', 'title', 'abstract', 'primary_category', 'categories', 'authors','created','updated','doi'))
    base_url = "http://export.arxiv.org/oai2?verb=ListRecords&"
    url = (base_url+"from="+start_date+"&until="+end_date+"&"+"metadataPrefix=arXiv&set=%s"%arxiv)
    while True:
        print ("fetching", url)
        try:
            response = urllib.request.urlopen(url)

        except Exception as e:
            print(e)
            print ("Got 503. Retrying after {0:d} seconds.".format(60))
            time.sleep(60)
            continue

        xml = response.read()

        root = ET.fromstring(xml)
        for record in root.find(OAI+'ListRecords').findall(OAI+"record"):
            # HEADER
            try:
                setSpec = record.find(OAI+'header').find(OAI+'setSpec').text
            except:
                setSpec = " "
            # METADATA
            try:
                meta = record.find(OAI+'metadata')
            except:
                meta = " "
            try:
                info = meta.find(ARXIV+"arXiv")
            except:
                info = " "
            try:
                arxiv_id = info.find(ARXIV+"id").text
            except:
                arxiv_id = " "
            try:
                created = info.find(ARXIV+"created").text
                
            except:
                created = " "    
            
            try:
                updated = info.find(ARXIV+"updated").text
                
            except Exception as e:
                 updated = " "
            
            authors = []
            for i in info.findall(ARXIV+"authors"):
                for j in i.findall(ARXIV+"author"):
                    try:
                         for keyname, forenames in zip(j.findall(ARXIV+"keyname"), j.findall(ARXIV+"forenames")):
                            authors.append(keyname.text +" "+forenames.text)
                    except:
                        authors = " "
            try:
                categories = info.find(ARXIV+"categories").text
            except:
                categories = " "
            doi = info.find(ARXIV+"doi")
            if doi is not None:
                doi = doi.text.split()[0]
            contents = {'arxiv_id' : arxiv_id,
                        'title': info.find(ARXIV+"title").text,
                        'abstract': info.find(ARXIV+"abstract").text.strip(),
                        'primary_category' : setSpec,
                        'categories': categories.split(),
                        'authors' : authors,
                        'created': created,
                        'updated' : updated,
                        'doi': doi
                                }

            df = df.append(contents, ignore_index=True)
            del authors

            # The list of articles returned by the API comes in chunks of
            # 1000 articles. The presence of a resumptionToken tells us that
            # there is more to be fetched.
        token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
        if token is None or token.text is None:
            break

        else:
            url = base_url + "resumptionToken=%s"%(token.text)      
    return df



if __name__ == "__main__":
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    remaning = sys.argv[3]
    categories = ['eess', 'econ', 'math', 'cs', 'physics', 'physics:astro-ph', 'physics:cond-mat', 'physics:gr-qc', 'physics:hep-ex', 'physics:hep-lat', 'physics:hep-ph', 'physics:hep-th', 'physics:math-ph', 'physics:nlin', 'physics:nucl-ex', 'physics:nucl-th', 'physics:physics', 'physics:quant-ph', 'q-bio', 'q-fin', 'stat']
    data = pd.DataFrame()
    try: 
        for category in categories:
            try:
                print("Doing for :", category)
                data = data.append(harvest(category ,start_date, end_date), ignore_index=True)
            except Exception as e:
                print(e)
                pass
        data = data.drop_duplicates(subset=["arxiv_id"], keep="first")
        data.to_json("./data/"+start_date+remaning+".json",orient='records', lines=True)
        print("Saved Json file for",start_date+"-"+end_date )
    except Exception as e:
        print (e)

