import sys
import logging
import pandas as pd
import sqlite3
import os.path

# functions
def input_arguments():
    args = sys.argv
    db_path = "./dev/"
    csv_path = "./csv/"
    default_db = db_path + "cademycode.db"
    default_csv = csv_path + "out.csv"

    if len(args) <= 1:
        logging.warning("No arguments entered : Using defaults")
        db_fl = default_db
        csv_fl = default_csv
    elif len(args) == 2:
        logging.warning("No output entered : Using default")
        db_fl = db_path + args[1]
        csv_fl = default_csv
    else:
        db_fl = db_path + args[1]
        csv_fl = csv_path + args[2]
        logging.info(f"db : {db_fl} \told csv : {csv_fl}")

    if (does_file_exist(db_fl) and does_file_exist(csv_fl)):
        return db_fl, csv_fl
    
    logging.critical("EXITING : Error loading files")
    return 

def does_file_exist(fl):
    if not (os.path.isfile(fl)):
        logging.critical(f"No such file : {fl}")
        return False
    return True

def set_up_logging():
    log_path = "./logs/"
    file_name = "filelogtest.log"

    logging.basicConfig(
        filename=log_path + file_name, 
        level=logging.INFO,
        format='%(levelname)s:%(asctime)s:%(message)s') 
    logging.info("Logging set up\n")
    
def load_db(filename):
    try:
        conn = sqlite3.connect(filename)
    except:
        logging.critical(f"Error loading database file : {filename}")
        return
    curs = conn.cursor()
    curs.execute("""SELECT * FROM sqlite_master WHERE type='table'""")
    df_students = pd.read_sql_query("SELECT uuid, name, dob, sex, contact_info, job_id, num_course_taken, current_career_path_id, time_spent_hrs FROM cademycode_students", conn)
    df_courses = pd.read_sql_query("SELECT career_path_id, career_path_name, hours_to_complete FROM cademycode_courses", conn)
    df_stud_jobs = pd.read_sql_query("SELECT job_id, job_category, avg_salary FROM cademycode_student_jobs", conn)
    logging.info("db loaded and collated into dataframes completed")
    conn.close()

    return [df_students, df_stud_jobs, df_courses]

def load_csv(filename):
    try: 
        df = pd.read_csv(filename)
    except:
        logging.critical(f"Error loading CSV file : {filename}")
        return
    return df

def convert_db_to_csv(list_of_df):
    df_students, df_stud_jobs, df_courses = list_of_df
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
    df_complete = df_complete.drop(['name', 'contact_info', 'job_id', 'current_career_path_id', 'career_path_id'], axis=1)

    logging.info("CSV conversion completed")

    return df_complete


# 'main' function
def main():
    set_up_logging()

    db_fl, csv_fl = input_arguments()
    df_list = load_db(db_fl)
    if df_list is None:
        logging.critical("EXITED : Error loading database")
        return
    
    old_df = load_csv(csv_fl)
    if old_df is None:
        logging.critical("EXITED : Error loading csv")
        return
    
    new_df = convert_db_to_csv(df_list)

    length_diff = len(new_df) - len(old_df)
    logging.info(f"Added {length_diff} rows")

    out_csv = './csv/out.csv'
    new_df.to_csv(out_csv)
    logging.info(f"New CSV saved to {out_csv}\n  Process COMPLETE\n")

# run prog
if __name__ == '__main__':
    main()