import sqlite3

# DB_PATH = "db/annotation.db"
DB_PATH = "testdb/testdb.db"

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
    try:
        cursor = conn.cursor()
        for u in users:
            cursor.execute("""
                INSERT OR IGNORE INTO "User" (Email, Alias, PairEmail)
                VALUES (?, ?, ?)
            """, (u["email"], u["alias"], u["pair_email"]))
        conn.commit()
        print("Users inserted successfully.")
        conn.close()
    except sqlite3.Error as e:
        print(f"Error inserting users: {e}")


if __name__ == "__main__":
    insert_users(users)