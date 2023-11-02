import os
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from index_generator_v3 import Index_generator

# load environment variables
load_dotenv()
ES_URL =  os.environ["ES_URL"] if "ES_URL" in os.environ \
    else "http://bap.sparcs.org:31007"

if __name__ == '__main__':
    generator = Index_generator()

    # if index already exists, delete it
    res = requests.head("{}/{}".format(ES_URL, generator.index_name))
    if res.status_code == 200: requests.delete("{}/{}".format(ES_URL, generator.index_name))

    # create index
    res = requests.put("{}/{}".format(ES_URL, generator.index_name), json=generator.settings)
    if res.status_code != 200: raise Exception(res.text)

    # insert data
    inseted_count = 0
    docs = generator.get_parsed_docs()
    for doc in tqdm(docs, total=len(docs)):
        if doc == None: continue
        try:
            res = requests.post("{}/{}/_doc".format(ES_URL, generator.index_name), json=doc)
            inseted_count += 1 if res.status_code == 201 else 0
        except Exception as e:
            print(doc)
            print(e)
    print("A total of {} documents are added to {}".format(inseted_count, generator.index_name))
