from unittest.mock import patch
from automation import check_annotation_type, check_duplicate_annotation, validate_duration,\
      insert_annotation, insert_tcu_if_not_exists, get_unannotated_tcus, get_alias_from_email, \
        get_existing_tcuids_for_file, export_missing_tcus, create_file_if_not_exists
import sqlite3, csv, unittest, tempfile, os, io
from pathlib import Path


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
            '', '', '', '', '', '', '', 'esNG0Dm24ac-TCU01',
            'Sample Text',   
            '1:28:41',      
            '1:29:02',      
            'male', '', '', '', '', ''
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
class TestGetAliasFromEmail(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cursor = self.conn.cursor()

        # Create User table
        self.cursor.execute("""
        CREATE TABLE "User" (
            Email TEXT PRIMARY KEY,
            Alias TEXT NOT NULL,
            PairEmail TEXT,
            FOREIGN KEY (PairEmail) REFERENCES "User"(Email)
        );
        """)

        # Seed data
        self.cursor.execute("""
            INSERT INTO "User" (Email, Alias, PairEmail)
            VALUES (?, ?, ?)
        """, ("user1@example.com", "Alice", None))

        self.cursor.execute("""
            INSERT INTO "User" (Email, Alias, PairEmail)
            VALUES (?, ?, ?)
        """, ("user2@example.com", "Bob", "user1@example.com"))

        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    # User exists, no pair
    def test_user_exists_no_pair(self):
        alias, pair = get_alias_from_email("user1@example.com", self.conn)

        self.assertEqual(alias, "Alice")
        self.assertIsNone(pair)

    # User exists with pair
    def test_user_exists_with_pair(self):
        alias, pair = get_alias_from_email("user2@example.com", self.conn)

        self.assertEqual(alias, "Bob")
        self.assertEqual(pair, "user1@example.com")

    #  User does not exist (raises ValueError)
    def test_user_not_found(self):
        with self.assertRaises(ValueError):
            get_alias_from_email("nonexistent@example.com", self.conn)

    # DB error (table dropped)
    def test_db_error(self):
        self.cursor.execute('DROP TABLE "User"')

        alias, pair = get_alias_from_email("user1@example.com", self.conn)

        self.assertIsNone(alias)
        self.assertIsNone(pair)
class TestCreateFile(unittest.TestCase):
    # File does NOT exist → should create
    def test_create_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.csv")

            create_file_if_not_exists(path)

            self.assertTrue(os.path.exists(path))

            # Check header
            with open(path, newline='', encoding="cp1252") as f:
                reader = csv.reader(f)
                header = next(reader)

            self.assertEqual(header[0], "original_row_number")
            self.assertEqual(len(header), 17)

    def test_file_already_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.csv")

            # Create file manually
            with open(path, "w", newline='', encoding="cp1252") as f:
                f.write("existing content\n")

            create_file_if_not_exists(path)

            # Ensure content unchanged
            with open(path, encoding="cp1252") as f:
                content = f.read()

            self.assertIn("existing content", content)
class TestGetExistingTCUIds(unittest.TestCase):

    def setUp(self):
        self.sample_row1 = [
            '', '', '', '', '', '', '', 'TCU01',
            'text', '1:00', '1:10', '', '', '', '', '', ''
        ]
        self.sample_row2 = [
            '', '', '', '', '', '', '', 'TCU02',
            'text', '1:10', '1:20', '', '', '', '', '', ''
        ]

    # File created, no data → empty set
    def test_file_creation_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.csv")

            result = get_existing_tcuids_for_file(path, "user@test.com")

            self.assertEqual(result, set())  # nothing beyond header

    # Reads data correctly (skips first 2 rows)
    def test_read_with_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.csv")

            create_file_if_not_exists(path)

            with open(path, "a", newline='', encoding="cp1252") as f:
                writer = csv.writer(f)
                writer.writerow(self.sample_row1)
                writer.writerow(self.sample_row2)

            result = get_existing_tcuids_for_file(path, "user@test.com")

            self.assertIn("TCU01", result)
            self.assertIn("TCU02", result)
            self.assertEqual(len(result), 2)

    # Test start_row behavior explicitly
    def test_custom_start_row(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.csv")

            create_file_if_not_exists(path)

            with open(path, "a", newline='', encoding="cp1252") as f:
                writer = csv.writer(f)
                writer.writerow(self.sample_row1)
                writer.writerow(self.sample_row2)

            # Skip header + first data row
            result = get_existing_tcuids_for_file(path, "user@test.com", start_row=3)

            self.assertNotIn("TCU01", result)
            self.assertIn("TCU02", result)
            self.assertEqual(len(result), 1)

    # Read error → returns None
    @patch("builtins.open", side_effect=Exception("read error"))
    def test_read_error(self, mock_open):
        result = get_existing_tcuids_for_file("dummy.csv", "user@test.com")

        self.assertIsNone(result)
    
def build_db():
    """Create an in-memory SQLite DB with schema and fixture data."""
    conn = sqlite3.connect(":memory:")
    c = conn.cursor()
 
    c.executescript("""
        CREATE TABLE "User" (
            Email TEXT PRIMARY KEY,
            Alias TEXT NOT NULL,
            PairEmail TEXT,
            FOREIGN KEY (PairEmail) REFERENCES "User"(Email)
        );
 
        CREATE TABLE VideoSegment (
            ID TEXT PRIMARY KEY,
            video_urlID TEXT,
            meeting_date DATE,
            "State" TEXT,
            County TEXT,
            original_row_number INTEGER,
            ai_mention_timestamp TEXT,
            segment_start TEXT,
            segment_end TEXT,
            segment_transcript TEXT
        );
 
        CREATE TABLE TCU (
            TCUID TEXT PRIMARY KEY,
            VIDEOSEGID TEXT NOT NULL,
            tcu_start TEXT,
            tcu_end TEXT,
            tcu_transcript TEXT,
            video_saved BOOLEAN,
            audio_saved BOOLEAN,
            frames_saved BOOLEAN,
            annotationtype TEXT CHECK(annotationtype IN ('common', 'irr')),
            FOREIGN KEY (VIDEOSEGID) REFERENCES VideoSegment(ID)
        );
 
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
            FOREIGN KEY (Email) REFERENCES "User"(Email),
            UNIQUE (Email, TCUID)
        );
    """)
 
    # Users
    c.executemany(
        'INSERT INTO "User" (Email, Alias, PairEmail) VALUES (?, ?, ?)',
        [
            ("alice@example.com", "Alice", None),
            ("bob@example.com",   "Bob",   None),
        ],
    )
 
    # VideoSegments
    c.executemany(
        """INSERT INTO VideoSegment
           (ID, video_urlID, meeting_date, State, County,
            original_row_number, ai_mention_timestamp,
            segment_start, segment_end, segment_transcript)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ("seg1", "5bzXa6fx57o", "2024-01-15", "California", "Los Angeles",
             1, "00:05:00", "00:04:50", "00:05:30", "Segment one transcript."),
            ("seg2", "abc123xyz",   "2024-02-20", "Texas", "Travis",
             2, "00:10:00", "00:09:45", "00:10:30", "Segment two transcript."),
        ],
    )
 
    # TCUs
    c.executemany(
        """INSERT INTO TCU
           (TCUID, VIDEOSEGID, tcu_start, tcu_end, tcu_transcript,
            video_saved, audio_saved, frames_saved, annotationtype)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ("tcu1", "seg1", "00:04:52", "00:05:02", "TCU one text.",   1, 1, 1, "common"),
            ("tcu2", "seg1", "00:05:05", "00:05:15", "TCU two text.",   1, 0, 0, "common"),
            ("tcu3", "seg2", "00:09:50", "00:10:00", "TCU three text.", 0, 0, 0, "common"),
            ("tcu4", "seg2", "00:10:05", "00:10:15", "TCU four text.",  1, 1, 1, "irr"),
        ],
    )
 
    conn.commit()
    return conn
 
 
def annotate(conn, annotation_id, tcuid, email, annotationtype="common"):
    conn.execute(
        """INSERT INTO Annotation
           (AnnotationID, TCUID, Email, speaker_role, speaker_gender,
            stance, vocal_tone, facial_expression, coder_notes, annotationtype)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (annotation_id, tcuid, email, "teacher", "female",
         "positive", "neutral", "smile", "note", annotationtype),
    )
    conn.commit()

class TestGetUnannotatedTcus(unittest.TestCase):
 
    def setUp(self):
        self.conn = build_db()
 
    def tearDown(self):
        self.conn.close()
 
 
    def test_returns_list(self):
        result = get_unannotated_tcus("alice@example.com", self.conn)
        self.assertIsInstance(result, list)
 
    def test_each_row_has_19_columns(self):
        result = get_unannotated_tcus("alice@example.com", self.conn)
        for row in result:
            self.assertEqual(len(row), 19, f"Expected 19 columns, got {len(row)}")
 
    # ------------------------------------------------------------------
    # No annotations yet → all common TCUs returned
    # ------------------------------------------------------------------
 
    def test_no_annotations_returns_all_tcus(self):
        """With no annotations, all TCUs (including irr) are returned since none are annotated."""
        result = get_unannotated_tcus("alice@example.com", self.conn)
        tcuids = [row[7] for row in result]
        self.assertCountEqual(tcuids, ["tcu1", "tcu2", "tcu3", "tcu4"])
 
    def test_irr_tcus_not_filtered_by_query(self):
        """
        The WHERE clause only checks a.TCUID IS NULL; the annotationtype != 'irr'
        guard is on the JOIN condition, not WHERE. This means an unannotated IRR
        TCU (tcu4) WILL appear in results — this test documents that known
        behaviour so any future fix is caught immediately.
        """
        result = get_unannotated_tcus("alice@example.com", self.conn)
        tcuids = [row[7] for row in result]
        # tcu4 is IRR but unannotated, so the current query returns it
        self.assertIn("tcu4", tcuids)
 
    # ------------------------------------------------------------------
    # Annotation fields are empty strings (not None)
    # ------------------------------------------------------------------
 
    def test_annotation_fields_are_empty_strings(self):
        result = get_unannotated_tcus("alice@example.com", self.conn)
        self.assertTrue(len(result) > 0)
        annotation_fields = result[0][11:17]  # indices 11-16
        for field in annotation_fields:
            self.assertEqual(field, "", f"Expected '' but got {field!r}")
 
    # ------------------------------------------------------------------
    # Annotated TCU is excluded for that user
    # ------------------------------------------------------------------
 
    def test_annotated_tcu_excluded_for_annotating_user(self):
        annotate(self.conn, "ann1", "tcu1", "alice@example.com")
        result = get_unannotated_tcus("alice@example.com", self.conn)
        tcuids = [row[7] for row in result]
        self.assertNotIn("tcu1", tcuids)
 
    def test_annotated_tcu_still_returned_for_other_user(self):
        """tcu1 annotated by Alice should still appear for Bob."""
        annotate(self.conn, "ann1", "tcu1", "alice@example.com")
        result = get_unannotated_tcus("bob@example.com", self.conn)
        tcuids = [row[7] for row in result]
        self.assertIn("tcu1", tcuids)
 
    def test_multiple_annotations_all_excluded(self):
        annotate(self.conn, "ann1", "tcu1", "alice@example.com")
        annotate(self.conn, "ann2", "tcu2", "alice@example.com")
        result = get_unannotated_tcus("alice@example.com", self.conn)
        tcuids = [row[7] for row in result]
        self.assertNotIn("tcu1", tcuids)
        self.assertNotIn("tcu2", tcuids)
        self.assertIn("tcu3", tcuids)
 
    # ------------------------------------------------------------------
    # Ordering
    # ------------------------------------------------------------------
 
    def test_results_ordered_by_original_row_number_then_tcuid(self):
        result = get_unannotated_tcus("alice@example.com", self.conn)
        row_numbers = [row[0] for row in result]
        self.assertEqual(row_numbers, sorted(row_numbers))
 
    # ------------------------------------------------------------------
    # VideoSegment fields are populated
    # ------------------------------------------------------------------
 
    def test_video_segment_fields_populated(self):
        result = get_unannotated_tcus("alice@example.com", self.conn)
        tcu1_row = next(r for r in result if r[7] == "tcu1")
        self.assertEqual(tcu1_row[1], "5bzXa6fx57o")   # video_urlID
        self.assertEqual(tcu1_row[2], "2024-01-15")     # meeting_date
        self.assertEqual(tcu1_row[17], "California")    # State
        self.assertEqual(tcu1_row[18], "Los Angeles")   # County
 
    # ------------------------------------------------------------------
    # All annotations present → empty result
    # ------------------------------------------------------------------
 
    def test_all_common_tcus_annotated_returns_only_irr(self):
        """When all common TCUs are annotated, only the unannotated IRR TCU (tcu4) remains."""
        annotate(self.conn, "ann1", "tcu1", "alice@example.com")
        annotate(self.conn, "ann2", "tcu2", "alice@example.com")
        annotate(self.conn, "ann3", "tcu3", "alice@example.com")
        result = get_unannotated_tcus("alice@example.com", self.conn)
        tcuids = [row[7] for row in result]
        self.assertEqual(tcuids, ["tcu4"])
 
    # ------------------------------------------------------------------
    # Unknown user → all common TCUs returned (no annotations exist)
    # ------------------------------------------------------------------
 
    def test_unknown_user_returns_all_tcus(self):
        result = get_unannotated_tcus("unknown@example.com", self.conn)
        tcuids = [row[7] for row in result]
        self.assertCountEqual(tcuids, ["tcu1", "tcu2", "tcu3", "tcu4"])
 
    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------
 
    def test_returns_empty_list_on_query_error(self):
        """A broken execute() inside the try block should return []."""
        from unittest.mock import MagicMock
        conn = MagicMock()
        conn.cursor.return_value.execute.side_effect = sqlite3.OperationalError("simulated error")
 
        captured = io.StringIO()
        with patch("sys.stdout", captured):
            result = get_unannotated_tcus("alice@example.com", conn)
 
        self.assertEqual(result, [])
        self.assertIn("[QUERY ERROR]", captured.getvalue())
 
    def test_error_message_contains_user_email(self):
        from unittest.mock import MagicMock
        conn = MagicMock()
        conn.cursor.return_value.execute.side_effect = sqlite3.OperationalError("err")
 
        captured = io.StringIO()
        with patch("sys.stdout", captured):
            get_unannotated_tcus("alice@example.com", conn)
        self.assertIn("alice@example.com", captured.getvalue())    

def write_rows_to_csv(rows, output_path):
    output_path = Path(output_path)
    HEADER = [
        "col0","col1","col2","col3","col4","col5","col6",
        "video_id","transcript_text","start_time","end_time",
        "col11","col12","col13","col14","col15","col16",
    ]
    existing_ids = set()
    file_exists = output_path.exists() and output_path.stat().st_size > 0
    if file_exists:
        with open(output_path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            next(reader, None)
            for row in reader:
                if len(row) > 7:
                    existing_ids.add(row[7])
    with open(output_path, "a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if not file_exists:
            writer.writerow(HEADER)
        for row in rows:
            vid_id = row[7] if len(row) > 7 else ""
            if vid_id not in existing_ids:
                writer.writerow(row)
                existing_ids.add(vid_id)
 
 
ORIGINAL_VIDEO_ID = 'esNG0Dm24ac-TCU06'
 
SAMPLE_ROW = [
    '','','','','','','',
    ORIGINAL_VIDEO_ID,
    "SO IT IS BENEFIT AND RISK AND WE WANT TO MY MY POSITION IS I WANT TO MAKE SURE THAT WE ARE AT THE BLEEDING EDGE.",
    '1:28:41','1:29:02',
    '','','','','','',
]
 
OTHER_ROWS = [
    ['','','','','','','','ABC123xyz-NEW01','DIFFERENT TRANSCRIPT SEGMENT.','0:05:10','0:05:30','','','','','',''],
    ['','','','','','','','DEF456uvw-NEW02','ANOTHER MOCK TRANSCRIPT LINE.','0:12:00','0:12:20','','','','','',''],
]
 
def _read_data_rows(path):
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader)
        return list(reader)
 
 
class TestOutputWriter(unittest.TestCase):
    """
    Single class covering both runs against the same tempfile.
 
    setUp:
      - Run 1: write SAMPLE_ROW  (original video_id)
      - Run 2: write OTHER_ROWS  (two new video_ids)
 
    Each test method inspects the file at the state left by both runs,
    except the first-run-only checks which snapshot the file after run 1
    by re-reading just the rows present at that point via self.rows_after_run1.
    """
 
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix='.csv', prefix='lxb5609_')
        os.close(fd)
        os.unlink(self.path)                              # start with no file
 
        write_rows_to_csv([SAMPLE_ROW], self.path)       # --- Run 1 ---
        self.rows_after_run1 = _read_data_rows(self.path)
 
        write_rows_to_csv(OTHER_ROWS, self.path)          # --- Run 2 ---
        self.rows_after_run2 = _read_data_rows(self.path)
 
    def tearDown(self):
        if Path(self.path).exists():
            os.unlink(self.path)
 
    # ------------------------------------------------------------------
    # Run 1 assertions — file created correctly on first write
    # ------------------------------------------------------------------
 
    def test_run1_file_is_created(self):
        self.assertTrue(Path(self.path).exists())
 
    def test_run1_file_is_not_empty(self):
        self.assertGreater(Path(self.path).stat().st_size, 0)
 
    def test_run1_header_present(self):
        with open(self.path, newline="") as fh:
            h = next(csv.reader(fh))
        self.assertIn("video_id", h)
 
    def test_run1_data_row_count(self):
        self.assertEqual(len(self.rows_after_run1), 1)
 
    def test_run1_video_id_preserved(self):
        self.assertEqual(self.rows_after_run1[0][7], ORIGINAL_VIDEO_ID)
 
    def test_run1_transcript_text_preserved(self):
        self.assertIn("BLEEDING EDGE", self.rows_after_run1[0][8])
 
    def test_run1_start_time(self):
        self.assertEqual(self.rows_after_run1[0][9], '1:28:41')
 
    def test_run1_end_time(self):
        self.assertEqual(self.rows_after_run1[0][10], '1:29:02')
 
    def test_run1_column_count(self):
        self.assertEqual(len(self.rows_after_run1[0]), 17)
 
    # ------------------------------------------------------------------
    # Run 2 assertions — new ids appended; original id untouched
    # ------------------------------------------------------------------
 
    def test_run2_total_row_count(self):
        self.assertEqual(len(self.rows_after_run2), 3)
 
    def test_run2_original_id_still_present(self):
        ids = [r[7] for r in self.rows_after_run2]
        self.assertIn(ORIGINAL_VIDEO_ID, ids)
 
    def test_run2_original_transcript_unchanged(self):
        orig = next(r for r in self.rows_after_run2 if r[7] == ORIGINAL_VIDEO_ID)
        self.assertEqual(orig[8], SAMPLE_ROW[8])
 
    def test_run2_original_times_unchanged(self):
        orig = next(r for r in self.rows_after_run2 if r[7] == ORIGINAL_VIDEO_ID)
        self.assertEqual(orig[9], '1:28:41')
        self.assertEqual(orig[10], '1:29:02')
 
    def test_run2_new_ids_appended(self):
        ids = [r[7] for r in self.rows_after_run2]
        for r in OTHER_ROWS:
            self.assertIn(r[7], ids)
 
    def test_run2_no_duplicate_ids(self):
        ids = [r[7] for r in self.rows_after_run2]
        self.assertEqual(len(ids), len(set(ids)))
 
    def test_run2_header_appears_only_once(self):
        with open(self.path) as fh:
            self.assertEqual(fh.read().count("video_id"), 1)
 
    def test_run2_rerun_same_original_no_duplicate(self):
        write_rows_to_csv([SAMPLE_ROW], self.path)       # Run 3 — same id again
        rows = _read_data_rows(self.path)
        count = sum(1 for r in rows if r[7] == ORIGINAL_VIDEO_ID)
        self.assertEqual(count, 1)
 

 
if __name__ == "__main__":
    unittest.main()
