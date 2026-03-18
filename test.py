from automationXLSX import check_annotation_type, check_duplicate_annotation, insert_annotation, validate_duration,\
      insert_annotation, insert_tcu_if_not_exists, get_unannotated_tcus, get_alias_from_email, \
        get_existing_tcuids_for_file, export_missing_tcus
import sqlite3, csv

db = "db/annotation.db"

users = {   "Kelly": {"email": "kkz5193@psu.edu", "alias": "Kelly", "pair_email": "xzx5141@psu.edu"},
            "Xinyu": {"email": "xzx5141@psu.edu", "alias": "Xinyu", "pair_email": "sks7267@psu.edu"},
            "Swara": {"email": "sks7267@psu.edu", "alias": "Swara", "pair_email": "kkz5193@psu.edu"},
            "Luke": {"email": "lxb5609@psu.edu", "alias": "Luke", "pair_email": ""},
            "James": {"email": "jpg6390@psu.edu", "alias": "James", "pair_email": ""}}

def get_data(file_path):
    try:
        with open(file_path, newline='', encoding='cp1252') as f:
            reader = csv.reader(f)
        
            return list(reader)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []
    

test = [{"input": 
            {"useremail": "lxb5609@psu.edu", 
             "row": ['62', 'https://youtu.be/Ni0EByhwhAE', '16-Oct-24', '0:07:16', 'NA', 'NA', 'NA', '', '', '', '', '', '', '', '', '', '']}, 
        'expected': 
            'None'},
        {"input": 
            {"useremail": "lxb5609@psu.edu", 
             "row": ['62', 'https://youtu.be/Ni0EByhwhAE', '16-Oct-24', '0:07:16', 'NA', 'NA', 'NA', '', '', '', '', 'male', 'king', '', '', '', '']}, 
        'expected':
            'common'}]

if __name__ == "__main__":
    user = "Luke"
    data = get_data(f"annotation-human/version2/{user}/{user}_annotation_file.csv")
    for test_case in test:
        useremail = test_case["input"]["useremail"]
        row = test_case["input"]["row"]
        expected = test_case["expected"]
        result = check_annotation_type(useremail, row)
        print(f"Test case for useremail='{useremail}' | expected='{expected}' | got='{result}'")