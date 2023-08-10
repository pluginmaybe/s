import unittest
import sys
import db_updater

class TestCSVUpdater(unittest.TestCase):
    
    def test_command_line_input_validation(self):
        sys.argv = ['db_updater.py', 'cademycode.db', 'out.csv']
        res = db_updater.input_arguments()
        sys.argv = ['db_updater.py', 'cademycode.db', 't.csv']
        res_2 = db_updater.input_arguments()
        sys.argv = ['db_updater.py']
        res_3 = db_updater.input_arguments()
        
        self.assertNotEqual(res, None)
        self.assertEqual(res_2, None)
        self.assertNotEqual(res_3, None)


    def test_does_file_exist(self):
        filename = "./dev/cademycode.db"

        res = db_updater.does_file_exist(filename)
        res_2 = db_updater.does_file_exist("blah")
        self.assertTrue(res)
        self.assertFalse(res_2)

    def test_schemas_of_loaded_dfs(self):
        course_cols = ['career_path_id', 'career_path_name', 'hours_to_complete']
        job_cols = ['job_id', 'job_category', 'avg_salary']
        stud_cols = ['uuid', 'name', 'dob', 'sex', 'contact_info', 'job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs']

        [st, jb, crs] = db_updater.load_db("./dev/cademycode.db")
        self.assertEqual(stud_cols, st.columns.to_list())
        self.assertEqual(job_cols, jb.columns.to_list())
        self.assertEqual(course_cols, crs.columns.to_list())     
   
        complete_cols = ['uuid', 'dob', 'sex', 'num_course_taken', 'time_spent_hrs', 'job_category', 'avg_salary', 'career_path_name', 'hours_to_complete']
        comp = db_updater.convert_db_to_csv([st, jb, crs])
        self.assertEqual(complete_cols, comp.columns.to_list())


if __name__ == '__main__':
    unittest.main()