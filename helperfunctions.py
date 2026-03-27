import csv
import pandas as pd
import re

def normalize_time(t):
    """
    Accepts any of these formats and returns HH:MM:SS:
      "1:14:56"
      "01:14:56"
      "14:56"
      "0 days 01:16:38"
      "1 days 01:16:38"
      "0 days 00:05:23.500000"
      "" / None / "NA"
    Returns None if unparseable.
    """
    if t is None:
        return None

    t = str(t).strip()

    if t == "" or t.lower() == "na":
        return None

    # Strip pandas timedelta prefix: "0 days 01:16:38"
    days = 0
    days_match = re.match(r"(-?\d+)\s+days?\s+(.*)", t, re.IGNORECASE)
    if days_match:
        days = int(days_match.group(1))
        t    = days_match.group(2).strip()

    # Strip sub-seconds: "01:16:38.500000" → "01:16:38"
    t = re.sub(r"\.\d+$", "", t)

    # Parse HH:MM:SS or MM:SS
    parts = t.split(":")
    try:
        if len(parts) == 3:
            h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        elif len(parts) == 2:
            h, m, s = 0, int(parts[0]), int(parts[1])
        else:
            return None
    except ValueError:
        return None

    # Roll up days into hours
    h += days * 24

    return f"{h:02}:{m:02}:{s:02}"


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

def getIndex(colName, dataType = "tcucsv"):
    if dataType == "tcucsv":
        COL_IDX = {
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
            "state": 17,
            "county": 18,
            "tcu_adder_email": 19
        }    
        
    return COL_IDX[colName]

def getItem(row, colName, dataType = "tcucsv"):
    if getIndex(colName, dataType) < len(row):
        return row[getIndex(colName, dataType)]
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
    

def xlsx_to_csv(input_file, output_file):
    df = pd.read_excel(input_file, engine="openpyxl")
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
