# -*- coding: utf-8 -*-
"""
Created on Sat Sep 22 00:13:08 2018

@author: Vuong BUI
"""
import os
from urllib.request import urlretrieve
import json
import pandas as pd


max_results = 150000
length_request = 1000
data_dir = "data"


def download_json(max_results, length_request, data_dir):
    # Download the json files
    base_url = "https://hudoc.echr.coe.int/app/query/results?\
query=contentsitename%3AECHR%20AND%20(NOT%20(doctype%3DPR%20OR%20doctype%3DHFCOMOLD%20OR%20doctype%3DHECOMOLD))&\
select={flds}&\
sort=kpdate%20Descending&\
rankingModelId=11111111-0000-0000-0000-000000000000&\
start={st}&\
length={length}"
    fields = ("itemid", "appno", "extractedappno", "typedescription", "Rank",
              "ecli", "applicability", "publishedby", "conclusion",
              "decisiondate", "kpthesaurus", "article", "kpdate", "docname",
              "issue", "respondent", "sclappnos", "scl", "originatingbody",
              "resolutionnumber", "meetingnumber", "externalsources",
              "doctype", "rulesofcourt", "documentcollectionid",
              "isplaceholder", "documentcollectionid2", "doctypebranch",
              "appnoparts", "representedby", "introductiondate",
              "application", "judgementdate", "ECHRRanking", "languagenumber",
              "sharepointid", "languageisocode", "resolutiondate",
              "separateopinion", "importance", "kpdateAsText", "reportdate",
              "referencedate")
    for start in range(0, max_results, length_request):
        request_url = base_url.format(flds=','.join(fields),
                                      st=start, length=length_request)
        outfilename = os.path.join(data_dir, "json",
                                   "{start}.json".format(start=start))
        urlretrieve(request_url, outfilename)


def merge_json_to_df(max_results, length_request, data_dir):
    df = pd.DataFrame()
    for start in range(0, max_results, length_request):
        filename = os.path.join(data_dir, "json",
                                "{start}.json".format(start=start))
        with open(filename, 'r', encoding='utf-8') as f:
            json_data = json.load(f, encoding='utf-8')
            for one_result in json_data['results']:
                df2 = pd.DataFrame([one_result['columns']])
                df = df.append(df2)
    return df


def main():
    # Download the json files
    download_json(max_results, length_request, data_dir)

    # Merge the json files into a pandas DataFrame
    df = merge_json_to_df(max_results, length_request, data_dir)

    # Save the DataFrame to a Feather file
    path = os.path.join(data_dir, 'echr.feather')
    df.to_feather(path)


main()
