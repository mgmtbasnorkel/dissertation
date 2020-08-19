#!/usr/bin/env python
# coding: utf-8

# # WEB Scraper AlienVault

# In[1]:


import matplotlib.pyplot as plt 


import pandas as pd

import numpy as np

import json


# # Web Scraper

# In[3]:


import html5lib
import urllib.request
import re
from bs4 import BeautifulSoup
import numpy


from urllib.request import Request, urlopen

# url = 'https://www.fool.ca/recent-headlines/'
# req = Request(url , headers={'User-Agent': 'Mozilla/5.0'})
# webpage = urlopen(req).read()

#development tests
#-> Check for link which do not start with http
# pulses_df_small_no = pulses_df_small.dropna(subset=['references'])
# pulses_df_small_no.loc[~pulses_df_small_no.references.apply(lambda x: x.startswith('http'))]

def get_html_text(url):

    forbiddenwords = re.compile('twitter.com') 
    if url!=url:
        return "No Link"
    elif url.startswith('http')==False:
        return "No Link"
    elif forbiddenwords.search(url): 
        try:
            req = Request(url , headers= {'User-Agent': 'Nokia5310XpressMusic_CMCC/2.0 (10.10) Profile/MIDP-2.1 '                                                    'Configuration/CLDC-1.1 UCWEB/2.0 (Java; U; MIDP-2.0; en-US; '                                                'Nokia5310XpressMusic) U2/1.0.0 UCBrowser/9.5.0.449 U2/1.0.0 Mobile'})

            html = urlopen(req).read().decode()

            soup = BeautifulSoup(html, "lxml")
            tt = soup.get_text(separator=" ")

            tt = re.sub(r'[\t\r\n]', '', tt)
            sentence = " ".join(re.split("\s+", tt, flags=re.UNICODE)).strip()

            sentence = re.search('Sign up(.*)Enter a topic', sentence).group(1) #keep main text

            return sentence
        except urllib.error.HTTPError as e:
             if e.getcode() == 404: # check the return code
                pass
    else:
        try:
            req = Request(url , headers= {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'})

            html = urlopen(req).read().decode()

            soup = BeautifulSoup(html, "lxml")
            tt = soup.get_text(separator=" ")

            tt = re.sub(r'[\t\r\n]', '', tt)
            sentence = " ".join(re.split("\s+", tt, flags=re.UNICODE)).strip()
            return sentence
        except:
            pass


# In[4]:


pulses_df = pd.read_pickle('pulses_df_with_desc_and_ref_STRAT.pkl')
pulses_ref_pass = pulses_df.loc[pulses_df.ref_pass==1]
pulses_ref_pass = pulses_ref_pass.loc[:,['SPLIT', 'reference_0', 'reference_1','reference_2', 'ref_pass']]
pulses_ref_pass = pulses_ref_pass.reset_index()


# In[5]:


# pulses_ref_pass = pulses_ref_pass.iloc[:10]


# In[6]:


get_html_text(pulses_ref_pass.reference_0[1])


# In[7]:


pulses_ref_pass['extracted_text'] = 0
pulses_ref_pass['extracted_text'] = pulses_ref_pass['reference_0'].apply(lambda x: get_html_text(x))


# In[8]:


pulses_ref_pass_ref_1 = pulses_ref_pass.loc[(pulses_ref_pass.reference_1.notnull() )& (pulses_ref_pass.extracted_text.isnull()) ]
pulses_ref_pass_ref_1['extracted_text'] = pulses_ref_pass_ref_1['reference_1'].apply(lambda x: get_html_text(x))


# In[9]:


pulses_ref_pass_ref_2 = pulses_ref_pass_ref_1.loc[(pulses_ref_pass_ref_1.reference_1.notnull() )& (pulses_ref_pass_ref_1.reference_2.notnull() ) & (pulses_ref_pass_ref_1.extracted_text.isnull()) ]
pulses_ref_pass_ref_2['extracted_text'] = pulses_ref_pass_ref_2['reference_2'].apply(lambda x: get_html_text(x))


# In[10]:


pulses_ref_pass_ref_1 = pulses_ref_pass_ref_1.loc[:,['ID','extracted_text'] ]
pulses_ref_pass_ref_1.columns = ['ID', 'extracted_text_1']

pulses_ref_pass_ref_2 = pulses_ref_pass_ref_2.loc[:,['ID','extracted_text'] ]
pulses_ref_pass_ref_2.columns = ['ID', 'extracted_text_2']


# In[12]:


pulses_ref_pass_final = pulses_ref_pass.merge(pulses_ref_pass_ref_1, on='ID', how='left').merge(pulses_ref_pass_ref_2, on='ID', how='left')
pulses_ref_pass_final = pulses_ref_pass_final.fillna("")
pulses_ref_pass_final['final'] = pulses_ref_pass_final['extracted_text'] +  pulses_ref_pass_final['extracted_text_1'] +  pulses_ref_pass_final['extracted_text_2']


# In[13]:


pulses_ref_pass_final['first_510'] = pulses_ref_pass_final.final.apply(lambda x: ' '.join(x.split()[:510]))
pulses_ref_pass_final['final_510'] = pulses_ref_pass_final.final.apply(lambda x: ' '.join(x.split()[-510:]))


# In[14]:


pulses_WEB_scraped = pulses_ref_pass_final.loc[:,['ID','SPLIT','reference_0', 'reference_1', 'reference_2','final', 'first_510', 'final_510']]
pulses_WEB_scraped.to_pickle('pulses_WEB_scraped.pkl')


# In[16]:


#pd.read_pickle('pulses_WEB_scraped.pkl')

