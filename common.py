import os
import time

import pandas as pd
import pymysql
from sqlalchemy import create_engine


def get_data_loc(file_name):
    parent_dir_path = os.path.abspath(os.getcwd())
    return parent_dir_path + "\\" + file_name


def get_engine(db, user, pw):
    return create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(
        user=user,
        pw=pw,
        db=db
    ))


def read_excel_data(filename):
    file_path = get_data_loc(filename)
    return pd.read_excel(r'{file_path}'.format(file_path=file_path))


def get_connection_object_and_cursor():
    """
    TODO: convert to context manager
    """
    connection_object = pymysql.connect(
        host="localhost", user="root", password="Bohemianrhapsody@14",
        db="data_analysis_project"
    )
    return connection_object, connection_object.cursor()


def print_answer_for_query(cursor_object, query, is_batch=False, callback_function=None):
    cursor_object.execute(query)
    if is_batch:
        query_result = cursor_object.fetchall()
    else:
        query_result = cursor_object.fetchone()

    if callback_function:
        callback_function(query_result)
    else:
        for row in query_result:
            print("Answer:", row)


def query_runner(query_inputs):
    connection_object, cursor_object = get_connection_object_and_cursor()
    try:
        for query_input in query_inputs:
            question_text = query_input.get('question_text')
            if question_text:
                print("Question: {question_text}".format(question_text=question_text))
            start = time.time()
            print_answer_for_query(cursor_object, query_input['query'], **query_input['kwargs'])
            end = time.time()
            print("Query time: {t}".format(t=end - start))
            print("\n")
    except Exception as e:
        print("Exception occurred:{}".format(e))
    finally:
        connection_object.close()