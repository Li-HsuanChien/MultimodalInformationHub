from automation import check_annotation_type, check_duplicate_annotation, validate_duration,\
      insert_annotation, insert_tcu_if_not_exists, get_unannotated_tcus, get_alias_from_email, \
        get_existing_tcuids_for_file, export_missing_tcus
import sqlite3, csv, unittest


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

class TestCheckAnnotationType(unittest.TestCase):

    def setUp(self):
        # Shared test cases
        self.test_cases = [
            {
                "input": {
                    "useremail": "lxb5609@psu.edu",
                    "row": ['62', 'https://youtu.be/Ni0EByhwhAE', '16-Oct-24', '0:07:16',
                            'NA', 'NA', 'NA', '', '', '', '', '', '', '', '', '', '']
                },
                "expected": 'None'
            },
            {
                "input": {
                    "useremail": "lxb5609@psu.edu",
                    "row": ['62', 'https://youtu.be/Ni0EByhwhAE', '16-Oct-24', '0:07:16',
                            'NA', 'NA', 'NA', '', '', '', '', 'male', 'king', '', '', '', '']
                },
                "expected": 'common'
            },
            {
                "input": {
                    "useremail": "sks7267@psu.edu",
                    "row": ['62', 'https://youtu.be/Ni0EByhwhAE', '16-Oct-24', '0:07:16',
                            'NA', 'NA', 'NA', '', '', '', '', '', '', '', '', '', '']
                },
                "expected": 'None'
            },
            {
                "input": {
                    "useremail": "sks7267@psu.edu",
                    "row": ['62', 'https://youtu.be/Ni0EByhwhAE', '16-Oct-24', '0:07:16',
                            'NA', 'NA', 'NA', '', '', '', '', '', '', '', '', '0', '']
                },
                "expected": 'common'
            },
            {
                "input": {
                    "useremail": "sks7267@psu.edu",
                    "row": ['62', 'https://youtu.be/Ni0EByhwhAE', '16-Oct-24', '0:07:16',
                            'NA', 'NA', 'NA', '', '', '', '', '', '', '0', '', '', '']
                },
                "expected": 'irr'
            }
        ]

    def test_annotation_types(self):
        for i, t in enumerate(self.test_cases):
            with self.subTest(i=i, input=t["input"]):
                result = check_annotation_type(
                    t["input"]["useremail"],
                    t["input"]["row"]
                )
                self.assertEqual(result, t["expected"])

class TestCheckDuplicateAnnotation(unittest.TestCase):

    def setUp(self):
        # Create in-memory database
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cursor = self.conn.cursor()

        self.cursor.execute("""
        CREATE TABLE "User" (
            Email TEXT PRIMARY KEY,
            Alias TEXT NOT NULL,
            PairEmail TEXT,
            FOREIGN KEY (PairEmail) REFERENCES "User"(Email)
        );
        """)

        self.cursor.execute("""
        CREATE TABLE VideoSegment (
            ID TEXT PRIMARY KEY
        );
        """)

        self.cursor.execute("""
        CREATE TABLE TCU (
            TCUID TEXT PRIMARY KEY,
            VIDEOSEGID TEXT NOT NULL,
            FOREIGN KEY (VIDEOSEGID) REFERENCES VideoSegment(ID)
        );
        """)

        self.cursor.execute("""
        CREATE TABLE Annotation (
            AnnotationID TEXT PRIMARY KEY,
            TCUID TEXT NOT NULL,
            Email TEXT NOT NULL,
            annotationtype TEXT CHECK(annotationtype IN ('common', 'irr')) NOT NULL,
            FOREIGN KEY (TCUID) REFERENCES TCU(TCUID),
            FOREIGN KEY (Email) REFERENCES "User"(Email),
            UNIQUE (Email, TCUID)
        );
        """)

        # Insert minimal valid data
        self.cursor.execute("INSERT INTO User (Email, Alias) VALUES (?, ?)", 
                            ("test@example.com", "tester"))

        self.cursor.execute("INSERT INTO VideoSegment (ID) VALUES (?)", 
                            ("vid1",))

        self.cursor.execute("INSERT INTO TCU (TCUID, VIDEOSEGID) VALUES (?, ?)", 
                            ("tcu1", "vid1"))

        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    #No duplicate exists
    def test_no_duplicate(self):
        result = check_duplicate_annotation("test@example.com", "tcu1", self.cursor)
        self.assertTrue(result)

    #Duplicate exists
    def test_duplicate_exists(self):
        # Insert annotation
        self.cursor.execute("""
            INSERT INTO Annotation (AnnotationID, TCUID, Email, annotationtype)
            VALUES (?, ?, ?, ?)
        """, ("ann1", "tcu1", "test@example.com", "common"))
        self.conn.commit()

        result = check_duplicate_annotation("test@example.com", "tcu1", self.cursor)
        self.assertFalse(result)

    #DB error (table missing)
    def test_db_error(self):
        # Drop table to force error
        self.cursor.execute("DROP TABLE Annotation")

        result = check_duplicate_annotation("test@example.com", "tcu1", self.cursor)
        self.assertFalse(result)

class TestInsertAnnotation(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cursor = self.conn.cursor()

        # Tables
        self.cursor.execute("""
        CREATE TABLE User (
            Email TEXT PRIMARY KEY
        );
        """)

        self.cursor.execute("""
        CREATE TABLE TCU (
            TCUID TEXT PRIMARY KEY
        );
        """)

        self.cursor.execute("""
        CREATE TABLE Annotation (
            AnnotationID TEXT PRIMARY KEY,
            TCUID TEXT NOT NULL,
            Email TEXT NOT NULL,
            speaker_role TEXT,
            speaker_gender TEXT,
            stance TEXT,
            vocal_tone TEXT,
            facial_expression TEXT,
            coder_notes TEXT,
            annotationtype TEXT CHECK(annotationtype IN ('common', 'irr')) NOT NULL,
            FOREIGN KEY (TCUID) REFERENCES TCU(TCUID),
            FOREIGN KEY (Email) REFERENCES User(Email),
            UNIQUE (Email, TCUID)
        );
        """)

        # Seed data
        self.cursor.execute("INSERT INTO User (Email) VALUES (?)", ("test@example.com",))
        self.cursor.execute("INSERT INTO TCU (TCUID) VALUES (?)", ("tcu1",))
        self.conn.commit()

        self.row = [
            '', '', '', '', '', '', '', '',
            'Sample Text',   
            '1:28:41',      
            '1:29:02',      
            '', '', '', '', '', ''
        ]

    def tearDown(self):
        self.conn.close()

    # Success case
    def test_insert_success(self):
        result = insert_annotation("ann1", "tcu1", "test@example.com", self.row, "common", self.cursor)
        self.conn.commit()

        self.assertTrue(result)

        self.cursor.execute("SELECT * FROM Annotation WHERE AnnotationID=?", ("ann1",))
        self.assertIsNotNone(self.cursor.fetchone())

    # Duplicate (Email + TCUID)
    def test_duplicate_annotation(self):
        insert_annotation("ann1", "tcu1", "test@example.com", self.row, "common", self.cursor)
        self.conn.commit()

        result = insert_annotation("ann2", "tcu1", "test@example.com", self.row, "common", self.cursor)
        self.conn.commit()

        self.assertFalse(result)

        self.cursor.execute("SELECT COUNT(*) FROM Annotation")
        count = self.cursor.fetchone()[0]
        self.assertEqual(count, 1)

    # Foreign key failure
    def test_foreign_key_failure(self):
        result = insert_annotation("ann3", "invalid_tcu", "test@example.com", self.row, "common", self.cursor)
        self.conn.commit()

        self.assertFalse(result)

        self.cursor.execute("SELECT * FROM Annotation WHERE AnnotationID=?", ("ann3",))
        self.assertIsNone(self.cursor.fetchone())

    # Invalid annotation type
    def test_invalid_annotation_type(self):
        result = insert_annotation("ann4", "tcu1", "test@example.com", self.row, "invalid", self.cursor)
        self.conn.commit()

        self.assertFalse(result)

        self.cursor.execute("SELECT * FROM Annotation WHERE AnnotationID=?", ("ann4",))
        self.assertIsNone(self.cursor.fetchone())

    # DB error (table dropped)
    def test_db_error(self):
        self.cursor.execute("DROP TABLE Annotation")

        result = insert_annotation("ann5", "tcu1", "test@example.com", self.row, "common", self.cursor)

        self.assertFalse(result)        
class TestInsertTCU(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cursor = self.conn.cursor()

        # Create tables
        self.cursor.execute("""
        CREATE TABLE VideoSegment (
            ID TEXT PRIMARY KEY
        );
        """)

        self.cursor.execute("""
        CREATE TABLE TCU (
            TCUID TEXT PRIMARY KEY,
            VIDEOSEGID TEXT NOT NULL,
            tcu_start TEXT,
            tcu_end TEXT,
            tcu_transcript TEXT,
            video_saved BOOLEAN,
            audio_saved BOOLEAN,
            frames_saved BOOLEAN,
            FOREIGN KEY (VIDEOSEGID) REFERENCES VideoSegment(ID)
        );
        """)

        # Insert dependency
        self.cursor.execute("INSERT INTO VideoSegment (ID) VALUES (?)", ("vid1",))
        self.conn.commit()

        # Sample row
        self.row = [
            '', '', '', '', '', '', '', '',
            'Sample Text',   
            '1:28:41',      
            '1:29:02',      
            '', '', '', '', '', ''
        ]

    def tearDown(self):
        self.conn.close()

    # Insert works
    def test_insert_success(self):
        result = insert_tcu_if_not_exists("tcu1", "vid1", self.row, self.cursor)
        self.conn.commit()

        self.assertTrue(result)

        self.cursor.execute("SELECT * FROM TCU WHERE TCUID=?", ("tcu1",))
        data = self.cursor.fetchone()
        self.assertIsNotNone(data)

    # Duplicate insert ignored
    def test_insert_duplicate(self):
        insert_tcu_if_not_exists("tcu1", "vid1", self.row, self.cursor)
        self.conn.commit()

        # Try inserting same again
        result = insert_tcu_if_not_exists("tcu1", "vid1", self.row, self.cursor)
        self.conn.commit()

        self.assertTrue(result)  # still True because IGNORE

        self.cursor.execute("SELECT COUNT(*) FROM TCU WHERE TCUID=?", ("tcu1",))
        count = self.cursor.fetchone()[0]
        self.assertEqual(count, 1)

    # Foreign key failure
    def test_foreign_key_failure(self):
        result = insert_tcu_if_not_exists("tcu2", "invalid_vid", self.row, self.cursor)
        self.conn.commit()

        # SQLite INSERT OR IGNORE → does NOT raise error, just ignores
        self.assertFalse(result)

        self.cursor.execute("SELECT * FROM TCU WHERE TCUID=?", ("tcu2",))
        data = self.cursor.fetchone()
        self.assertIsNone(data)

    #  DB error (table dropped)
    def test_db_error(self):
        self.cursor.execute("DROP TABLE TCU")

        result = insert_tcu_if_not_exists("tcu3", "vid1", self.row, self.cursor)

        self.assertFalse(result)
class TestValidateDuration(unittest.TestCase):

    def setUp(self):
        self.test_cases = [
            {
                "input": {
                    "row":['', '', '', '', '', '', '', 'esNG0Dm24ac-TCU06', "Sample Tesxt", '1:28:41', '1:29:02', '', '', '', '', '', '']
                },
                "expected": True
            },
            {
                "input": {
                    "row":['', '', '', '', '', '', '', 'esNG0Dm24ac-TCU06', "Sample Tesxt", '12841', '1:29:02', '', '', '', '', '', '']
                },
                "expected": (False, "FORMAT")
            },
            {
                "input": {
                    "row":['', '', '', '', '', '', '', 'esNG0Dm24ac-TCU06', "Sample Tesxt", 'NA', 'NA', '', '', '', '', '', '']
                },
                "expected": (False, "FORMAT")
            },
            {
                #less than 0 seconds
                "input": {
                    "useremail": "sks7267@psu.edu",
                    "row":['', '', '', '', '', '', '', 'esNG0Dm24ac-TCU06', "Sample Tesxt", '1:28:41', '1:27:01', '', '', '', '', '', '']
                },
                "expected": (False, "DURATION")   
            },
            {
                # 0 seconds
                "input": {
                    "useremail": "sks7267@psu.edu",
                    "row":['', '', '', '', '', '', '', 'esNG0Dm24ac-TCU06', "Sample Tesxt", '1:28:41', '1:28:41', '', '', '', '', '', '']
                },
                "expected": (False, "DURATION")   
            },
            {
                #longer than 60 seconds
                "input": {
                    "useremail": "sks7267@psu.edu",
                    "row":['', '', '', '', '', '', '', 'esNG0Dm24ac-TCU06', "Sample Tesxt", '1:28:41', '1:30:02', '', '', '', '', '', '']
                },
                "expected": (False, "DURATION")   
            }
        ]

    def test_duration(self):
        for i, t in enumerate(self.test_cases):
            with self.subTest(i=i, input=t["input"]):
                result = validate_duration(t["input"]["row"])
                self.assertEqual(result, t["expected"])

if __name__ == "__main__":
    unittest.main()
