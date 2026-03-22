import requests
import os
from dotenv import load_dotenv
from helperfunctions import xlsx_to_csv

load_dotenv()

def fetch_and_convert_files():
    for user in ["James", "Kelly", "Luke", "Swara", "Xinyu"]:
        file_url =os.path.join(os.getenv(f"{user}_FILE_URL"))
        try:
            if file_url != "":
                file_dir = f"annotation-human/version2/{user}"
                
                os.makedirs(file_dir, exist_ok=True)
                response = requests.get(file_url)
                if response.status_code == 200:
                    with open(f"{user}_file.xlsx", "wb") as f:
                        f.write(response.content)
                    xlsx_to_csv(f"{user}_file.xlsx", f"{file_dir}/{user}_annotation_file.csv")
                    os.remove(f"{user}_file.xlsx")
                    print(f"{user}'s file downloaded successfully.")
                else:
                    raise requests.RequestException(f"Failed to download {user}'s file. Status code: {response.status_code}")
        except requests.RequestException as e:
            print(f"Error occurred while fetching {user}'s file: {e}")

if __name__ == "__main__":
    fetch_and_convert_files()