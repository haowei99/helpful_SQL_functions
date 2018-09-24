# helpful_SQL_functions
This post is intended to list helpful functions associated with retrieving data from SQL using Python

Those code are written by myself with lots of experiement and testing. 

Feel free to use those functions as reference! :D

# Python SQL Functions

sql_read(conn_str, select_stmt, lst_data=None)

  - Read a sql data table and returns a list of a list with the data retrieved


sql_insert_db(conn_str, insert_stmt, ldata)

  - Insert ldata (can be a dictionary) to a desired table in a database

sql_nonquery_db(conn_str, stmt)

  - Do SQL commands that does not return a table (such as delete statement or reformatting table etc...)
  

sql_query_pandas(query)

  - Similar to SQL_read, but return data in a Pandas table
  
# Connecti string format
connectionString = 'DSN= ;  database=  ;UID=   ;PWD=   '

Please fill in the variables. Refer to your database engine. 
