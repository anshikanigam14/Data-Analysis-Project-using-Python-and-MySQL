import statistics
import pandas as pd

from common import get_engine, read_excel_data, get_connection_object_and_cursor, print_answer_for_query


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


def query_3_callback(query_result):
    for row in query_result:
        print("Answer:", row[1])


def query_5_callback(query_result):
    median_tat = statistics.median(query_result)
    print("Answer:", median_tat[0])

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
    print_answer_for_query(cursor_object, query_3, is_batch=True, callback_function=query_3_callback)

    query_4 = "select concat(round((count(distinct a.parentid)/(select count(distinct _id) from question_data)) * 100), '%') as percentage_of_questions_Ans from question_data q inner join answer_data a on q._id = a.parentid where TIMESTAMPDIFF(minute,q.creationdate,a.creationdate) > 5 OR  TIMESTAMPDIFF(second,q.creationdate,a.creationdate) > 300 ;"
    print_answer_for_query(cursor_object, query_4)

    query_5 = "select TIMESTAMPDIFF(second,q.creationdate,a.creationdate)  as tat from question_data q inner join answer_data a on q._id = a.parentid order by tat asc"
    print_answer_for_query(cursor_object, query_5, is_batch=True, callback_function=query_5_callback)

    query_6 = "with first_unanswered_qued as ( select  userid, _id, creationdate, RANK() OVER ( PARTITION BY userid order by CASE WHEN date(creationdate) THEN minute(creationdate) ELSE second(creationdate) end asc ) order_of_ques from question_data where _id not in (select parentid from answer_data)) select count(distinct _id) from first_unanswered_qued where order_of_ques = 1;"
    print_answer_for_query(cursor_object, query_6)

except Exception as e:
    print("Exception occurred:{}".format(e))
finally:
    connection_object.close()
