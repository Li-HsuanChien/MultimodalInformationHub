import csv


def time_to_seconds(t):
    try:
        h, m, s = map(int, t.split(":"))
        return h * 3600 + m * 60 + s
    except:
        return None
    
def extract_video_id(url):
    return url.replace("https://youtu.be/", "").strip()

def return_video_url(id):
    return f"https://youtu.be/{id}"

def build_videoseg_id(video_id, timestamp):
    return f"{video_id}-ai_{timestamp}"

def getindex(colName):
    User_Input_COL_IDX = {
        "original_row_number": 0,
        "video_url": 1,
        "meeting_date": 2,
        "ai_mention_timestamp": 3,
        "segment_start": 4,
        "segment_end": 5,
        "segment_transcript": 6,
        "tcu_id": 7,
        "tcu_transcript": 8,
        "tcu_start": 9,
        "tcu_end": 10,
        "speaker_role": 11,
        "speaker_gender": 12,
        "stance": 13,
        "vocal_tone": 14,
        "facial_expression": 15,
        "coder_notes": 16,
        "State": 17,
        "County": 18,
    }    
    return User_Input_COL_IDX[colName]

def getItem(row, colName):
    if getindex(colName) < len(row):
        return row[getindex(colName)]
    else:
        raise IndexError(f"Column '{colName}' is out of range for the provided row.") 
    
def getRequiredFields(user_email, type = "common"):
    requiredFields = {
        "kkz5193@psu.edu": ["stance"],
        "xzx5141@psu.edu": ["vocal_tone"],
        "sks7267@psu.edu": ["facial_expression"],
        "lxb5609@psu.edu": ["speaker_role", "speaker_gender"]
    }
    if type == "irr":
        requiredFields = {
            "kkz5193@psu.edu": ["vocal_tone"],
            "xzx5141@psu.edu": ["facial_expression"],
            "sks7267@psu.edu": ["stance"]
        }

    return requiredFields.get(user_email, [])

def read_data_rows(path):
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        next(reader)
        return list(reader)