import statistics
import pandas as pd
from pandas.io import sql
import xlrd
import pymysql
import os
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


def dump_xlsx_to_sql():
    engine = get_engine('data_analysis_project', 'root', 'Bohemianrhapsody@14')
    df1 = read_excel_data("questions_data.xlsx")
    df2 = read_excel_data("answers_data.xlsx")

    # converting unix epoch time to normal date time format
    df1['creationDate'] = pd.to_datetime(df1['creationDate'], unit='ms')
    df2['creationDate'] = pd.to_datetime(df2['creationDate'], unit='ms')
    # print(df1['creationDate'])
    # print(df2['creationDate'])

    df1.to_sql(con=engine, name='question_data', if_exists='replace')
    df2.to_sql(con=engine, name='answer_data', if_exists='replace')
    # print (df1)
    # print (df2)


dump_xlsx_to_sql()

#############################################################################################################################################################################

def get_connection_object_and_cursor():
    """
    TODO: convert to context manager
    """
    connection_object = pymysql.connect(
        host="localhost", user="root", password="Bohemianrhapsody@14",
        db="data_analysis_project"
    )
    return connection_object, connection_object.cursor()


def print_answer_for_query(cursor_object, query, is_batch=False):
    cursor_object.execute(query)
    if is_batch:
        query_result = cursor_object.fetchall()
    else:
        query_result = cursor_object.fetchone()

    for row in query_result:
        print("Answer:", row)


try:
    connection_object, cursor_object = get_connection_object_and_cursor()
    query_1 = """
        select count(_id) as total_answered_questions_deleted 
        from question_data where _id in (
            Select parentid from answer_data
        ) and isdeleted = 1;
    """

    print_answer_for_query(cursor_object, query_1)

    query_2 = "select count( _id) as total_answered_questions_anonymously from question_data where  _id in (Select parentid from answer_data) and isAnonymous = 1;"
    print_answer_for_query(cursor_object, query_2)

    query_3 = "select count(_id), date(creationdate) from question_data where isAnonymous = 1 group by date(creationdate) order by count(_id) desc limit 1;"
    print_answer_for_query(cursor_object, query_3, True)

    query_4 = "select concat(round((count(distinct a.parentid)/(select count(distinct _id) from question_data)) * 100), '%') as percentage_of_questions_Ans from question_data q inner join answer_data a on q._id = a.parentid where TIMESTAMPDIFF(minute,q.creationdate,a.creationdate) > 5 OR  TIMESTAMPDIFF(second,q.creationdate,a.creationdate) > 300 ;"
    print_answer_for_query(cursor_object, query_4)

    sqlQuery5= "select TIMESTAMPDIFF(second,q.creationdate,a.creationdate)  as tat from question_data q inner join answer_data a on q._id = a.parentid order by tat asc"
    cursor_object.execute(sqlQuery5)
    data5 = cursor_object.fetchall()
    median_tat = statistics.median(data5)
    print("Answer:", median_tat[0])

    query_6 = "with first_unanswered_qued as ( select  userid, _id, creationdate, RANK() OVER ( PARTITION BY userid order by CASE WHEN date(creationdate) THEN minute(creationdate) ELSE second(creationdate) end asc ) order_of_ques from question_data where _id not in (select parentid from answer_data)) select count(distinct _id) from first_unanswered_qued where order_of_ques = 1;"
    print_answer_for_query(cursor_object, query_6)

except Exception as e:
    print("Exception occurred:{}".format(e))
finally:
    connection_object.close()
