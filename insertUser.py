import sqlite3


DB_PATH = "annotation.db"
""" Kelly/ 
    Xinyu/ 
    Swara/
    Luke/ 
    James/

"""

conn = sqlite3.connect(DB_PATH)
users = [   {"email": "kkz5193@psu.edu", "alias": "Kelly", "pair_email": "xzx5141@psu.edu"},
            {"email": "xzx5141@psu.edu", "alias": "Xinyu", "pair_email": "sks7267@psu.edu"},
            {"email": "sks7267@psu.edu", "alias": "Swara", "pair_email": "kkz5193@psu.edu"},
            {"email": "lxb5609@psu.edu", "alias": "Luke", "pair_email": ""},
            {"email": "jpg6390@psu.edu", "alias": "James", "pair_email": ""}
]
def insert_users(users):
    """
        users = [
            {"email": "...", "alias": "...", "pair_email": "..."},
        ]
    """
    cursor = conn.cursor()
    for u in users:
        cursor.execute("""
            INSERT OR IGNORE INTO "User" (Email, Alias, PairEmail)
            VALUES (?, ?, ?)
        """, (u["email"], u["alias"], u["pair_email"]))
    conn.commit()
    