
# coding: utf-8

# # Staging Geo-coding test data
# 
# ref [GPC:ticket:140][140]
# 
# [140]: https://informatics.gpcnetwork.org/trac/Project/ticket/140
# 
# Subject: example data dictionary/codebook and data file  
# From: David Van Riper [mailto:vanriper@umn.edu]  
# Sent: Tuesday, July 07, 2015 1:34 PM  
# To: Mei Liu  
#  
# fwd by Mei Tuesday, July 07, 2015 1:54 PM

# In[1]:

import pandas as pd
import numpy as np
dict(pandas=pd.__version__,
     numpy=pd.__version__)


# ## Field Definitions

# In[2]:

def mn_census_data():
    from pathlib import Path
    return Path('/d1/geo-census/mn-census-data')

bk = pd.read_csv((mn_census_data() / 'acs_20135a/index_of_data_fields__acs_20135a.csv').open('rb'))
bk[['data_type', 'variable_code', 'variable_label']].head()


# In[3]:

bk.columns


# In[4]:

v = bk[bk.variable_code == 'UHD001'].iloc[0]
# v = bk[bk.variable_code == 'FILEID'].iloc[0]
v


# In[5]:

# bk[bk.variable_label.str.contains('ducation')][['data_type', 'table_label', 'variable_code', 'variable_label', 'table_universe', u'table_sequence', u'variable_sequence']]


# In[6]:

# bk[['table_source_code', 'table_label']].drop_duplicates().reset_index()


# ## Income Field
# 
# Desired fields, from #140:
# >  - Income, education, likelihood of employment, poverty status, owner-occupied house value, health insurance coverage, etc.
# 
# Let's start with just income by zip.

# In[7]:

# vars = bk[bk.variable_code.isin(['ZCTA5', 'UHD001', 'UG4011', 'UGS001', 'UGS012', 'UGS017'])]
income_vars = bk[bk.variable_code.isin(['ZCTA5', 'UHD001'])]

income_vars[['data_type', 'table_label', 'table_universe', 'variable_code', 'variable_label']]


# In[8]:

# Education
# ed = bk[bk.table_source_code.isin(['B15003'])]
# ed[['data_type', 'table_label', 'table_universe', 'variable_code', 'variable_label']]


# ## Data Files

# Data files are provided at various resolutions including state, county, zip code, all the way to the census block group:

# In[9]:

get_ipython().system(u'ls -s /d1/geo-census/mn-census-data/acs_20135a/*/*file.dat.gz | sort -n')


# We can decompress and pick out the first line...

# In[10]:

import gzip

# Zip code level data
zcta_860 = mn_census_data() / 'acs_20135a/zcta_860/ge.00_file.dat.gz'

line0 = gzip.GzipFile(zcta_860.name,
                      fileobj=zcta_860.open('rb')).readline()
line0[:40]


# ... and then pick out the median household income field (UHD001):

# In[11]:

line0[v.start_column - 1:v.start_column - 1 + v.width]


# Recall that's a numeric (n) field. Let's parse a dictionary record from a line and a selection of fields:

# In[12]:

SUPPRESSED = '.'  # per UMN folks

def parse_n(s, implied_decimal_places):
    return (float(s) / (10 ** implied_decimal_places)
            if s.strip() != SUPPRESSED
            else None)

def parse_field(s, f):
    return parse_n(s, f.implied_decimal_places) if f.data_type == 'n' else s

def parse_record(line, fields):
    return dict((f.variable_code,
                 parse_field(line[f.start_column - 1:f.start_column - 1 + f.width], f))
                for _, f in fields.iterrows())

parse_record(line0, bk[bk.variable_code.isin(['STUSAB', 'ZCTA5', 'UHD001'])])


# Now we can parse a record from each line in a data file and make a data frame:

# In[13]:

def load_lines(lines, fields):
    return pd.DataFrame([
            parse_record(line, fields)
            for (lineno, line) in enumerate(lines)
        ])



income_by_zip = load_lines(gzip.GzipFile(zcta_860.name, fileobj=zcta_860.open('rb')), income_vars).set_index('ZCTA5')
income_by_zip.head()


# In[14]:

income_vars.variable_label.iloc[1]


# In[16]:

from io import StringIO

patient_dimension = pd.read_csv(StringIO(u"""
patient_num,zip_code
1,64108
2,90210
""".strip()), index_col='patient_num', dtype=dict(zip_code='object'))
patient_dimension


# In[17]:

patient_dimension.merge(income_by_zip, how='left', left_on='zip_code', right_index=True)


# In[ ]:

http://www.oracle.com/technetwork/issue-archive/2011/11-mar/o21nanda-312277.html

