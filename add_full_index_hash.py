# -*- coding: utf-8 -*-
"""
Created on Wed Nov 29 09:57:10 2017
Small script to add hashes of indices to the DHS PSQL tables.
Intended to be run on the PSQL machine.
@author: Craig
"""

# The DHS tables, as inserted into our PSQL DB, are "chunked", that is, split
# by width since they have too many columns. So, e.g., we will have one table
# "DHS-Guatemala-Children Recode-v71", and then 5 more tables "D-G-CR-v71-N",
# N=1,...,5. Each of these tables has a set of columns serving as a common
# index, but attempting to join said tables in an automated fashion requires
# having a map from baseable to set-of-index-columns.
# Instead, we are going to add a new column, "full_index_hash", containing
# the hash of the index column values. This way, when we want to join, we can
# join on that one column alone and don't need the map.

import getpass
import re
from sqlalchemy import create_engine

# Index columns are identified by having one of these substrings.
INDEX_COLUMNS = [ "Facility number", "Unit line number", "unit type",
    "provider line number", "Case Identification", "Cluster number",
    "Household number", "Country code" ]


def main():
  pg_username = input("Please enter Postgres username:")
  pg_password = getpass.getpass("Password:")
    
  pg_login = pg_username + ":" + pg_password
  pg_conn_str = 'postgresql://' + pg_login + '@localhost:5432/dhs_data'
  engine = create_engine(pg_conn_str, echo=False, paramstyle='format')
  
  has_full_index_query = (
      'SELECT table_name '
      'FROM information_schema.columns '
      'WHERE table_name ILIKE \'DHS_%%\' '
      'AND column_name = \'full_index_hash\' '
      'ORDER BY 1 ASC')
  
  get_index_columns_query = (
      'SELECT table_name, column_name '
      'FROM information_schema.columns '
      'WHERE table_name ILIKE \'DHS_%%\' '
      'AND (column_name ILIKE \'%%')
  get_index_columns_query += '%%\' OR column_name ILIKE \'%%'.join(INDEX_COLUMNS)
  get_index_columns_query += '%%\') ORDER BY 1,2 ASC'

  table_queries = {}
  done_tables = set()
  
#  print(get_index_columns_query)
  with engine.connect() as con:
    done = con.execute(has_full_index_query)
    for row in done:
      done_tables.add(row['table_name'])
    
    res = con.execute(get_index_columns_query)
    for row in res:
      tname = row['table_name']
      if tname in done_tables: continue
      cname = '\"' + row['column_name'] + '\"'
      if re.search("number", cname, re.IGNORECASE):
        cname += '::text'
    
      if not tname in table_queries:
        table_queries[tname] = (
            'UPDATE \"' + tname + '\" '
            'SET full_index_hash = (\'x\'||substr(md5(' + cname
        )
      else:
        table_queries[tname] += '||' + cname
        
  
    for tname in table_queries:
      table_queries[tname] += ('),1,8))::bit(32)::int')
#      print(table_queries[tname])
      con.execute(
          'ALTER TABLE \"' + tname + '\" ADD COLUMN full_index_hash INT')
      con.execute(table_queries[tname])
      con.execute(
          'ALTER TABLE \"' + tname + '\" '
          'ALTER COLUMN full_index_hash SET NOT NULL')
    
    
if __name__ == '__main__':
  main()

      