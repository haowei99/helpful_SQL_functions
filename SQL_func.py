import pyodbc
import logging

"""Common functions related to querying SQL"""

def sql_read(conn_str, select_stmt, lst_data=None):
    '''
     general read function for SQL, return list of list of data
    :param select_stmt: select statement for SQL Server
    :return: table (nested list) of data
    '''

    cnxn = pyodbc.connect(conn_str)
    cursor = cnxn.cursor()

    try:

        if lst_data:
            cursor.execute(select_stmt, lst_data)
            dt = cursor.fetchall()
        else:
            cursor.execute(select_stmt)
            dt = cursor.fetchall()

        if not dt:
            logging.warning('no data read from ' + select_stmt + '!')
        return dt

    except:
        logging.exception('FAILED TO RETRIEVE DATA with ' + select_stmt)

    cnxn.close()


def sql_insert_db(conn_str, insert_stmt, ldata):
    '''
     loop through all files for today, write to sql
    :param insert_stmt: insert statement (missing parameters )
    :param ldata: list of values corresponding to list of keys included in insert_stmt
    '''

    cnxn = pyodbc.connect(conn_str)
    cursor = cnxn.cursor()
    try:
        cursor.execute(insert_stmt, ldata)
        status_bool = True
    except Exception as ex:
        logging.exception('FAILED TO INSERT/UPDATE DATA with ' + insert_stmt + '; ' + ', '.join(map(xstr, ldata)))
        print ex
        status_bool = False
    cnxn.commit()
    return status_bool


def sql_nonquery_db(conn_str, stmt):
    '''
     loop through all files for today, write to sql
    :param insert_stmt: insert statement (missing parameters )
    :param ldata: list of values corresponding to list of keys included in insert_stmt
    '''

    cnxn = pyodbc.connect(conn_str)
    cursor = cnxn.cursor()
    try:
        cursor.execute(stmt)
        result = cursor.rowcount
        if result:
            if (int(result) > 1):
                logging.info(str(result) + ' rows affected!')
        status_bool = True
    except Exception as ex:
        logging.exception('FAILED TO EXECUTE NON-QUERY with ' + stmt)
        print ex
        status_bool = False
    cnxn.commit()
    return status_bool


def sql_query_pandas(connstring, query):
    """Execute query in SQL that returns a dataframe and pandas (select *, etc)"""
    cnxn = pyodbc.connect(connectionString)
    cursor = cnxn.cursor()
    cursor.execute(query)
    value = cursor.fetchall()
    column_names = [i[0] for i in cursor.description]
    #print value

    df = DataFrame.from_records(value, columns=column_names)
    cnxn.close()

    return df
