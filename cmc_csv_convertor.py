
# read in db file and arrange into dataframes

import pandas as pd
import sqlite3

def csv_creator():
    conn = sqlite3.connect("./dev/cademycode.db")
    curs = conn.cursor()
    curs.execute("""SELECT * FROM sqlite_master WHERE type='table'""")
    df_students = pd.read_sql_query("SELECT uuid, name, dob, sex, contact_info, job_id, num_course_taken, current_career_path_id, time_spent_hrs FROM cademycode_students", conn)
    df_courses = pd.read_sql_query("SELECT career_path_id, career_path_name, hours_to_complete FROM cademycode_courses", conn)
    df_stud_jobs = pd.read_sql_query("SELECT job_id, job_category, avg_salary FROM cademycode_student_jobs", conn)
    conn.close()

    # drop redundant duplicate rows
    df_stud_jobs.drop([10,11,12], inplace=True, errors='ignore')

    # convert selected columns to more applicable datatypes
    cols_to_num = ['job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs']
    for c in cols_to_num:
      df_students[c] = pd.to_numeric(df_students[c])
    df_students['dob'] = pd.to_datetime(df_students['dob'])

    df_stud_jobs['job_id'] = df_stud_jobs['job_id'].astype('float')
    df_courses['career_path_id'] = df_courses['career_path_id'].astype('float')

    # merge the tables together and save as a csv

    full_df_a = df_students.merge(df_stud_jobs, left_on='job_id', right_on='job_id', how='left')
    df_complete = full_df_a.merge(df_courses, left_on='current_career_path_id', right_on='career_path_id', how='left')

    df_complete.to_csv('./out.csv')

    return "CSV creation complete."