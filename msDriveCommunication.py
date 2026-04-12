import requests
import os
from dotenv import load_dotenv
from helperfunctions import table_file_to_csv, getPartnerAlias

load_dotenv()

def fetch_and_convert_files():
    for user in ["James", "Kelly", "Luke", "Swara", "Xinyu"]:
        main_file_url =os.path.join(os.getenv(f"{user}_FILE_URL"))
        file_dir = f"annotation-human/version2/{user}"
        os.makedirs(file_dir, exist_ok=True)
        try:
            if main_file_url != "" and main_file_url is not None:                
                response = requests.get(main_file_url)
                if response.status_code == 200:
                    with open(f"{user}_file.xlsx", "wb") as f:
                        f.write(response.content)
                    table_file_to_csv(f"{user}_file.xlsx", f"{file_dir}/{user}_annotation_file.csv")
                    os.remove(f"{user}_file.xlsx")
                    print(f"{user}'s main file downloaded successfully.")
                else:
                    raise requests.RequestException(f"Failed to download {user}'s main file. Status code: {response.status_code}")
        except Exception as e:
            print(f"An error occurred while processing {user}'s main file: {e}")
        except requests.RequestException as e:
            print(f"Error occurred while fetching {user}'s main file: {e}")
        
        combined_file_url = os.getenv(f"{user}_COMBINED_URL")
        try:
            if combined_file_url != "" and combined_file_url is not None:                
                response = requests.get(combined_file_url)
                if response.status_code == 200:
                    with open(f"{user}_combined_all.xlsx", "wb") as f:
                        f.write(response.content)
                    table_file_to_csv(f"{user}_combined_all.xlsx", f"{file_dir}/combined_all.csv")
                    os.remove(f"{user}_combined_all.xlsx")
                    print(f"{user}'s combined file downloaded successfully.")
                else:
                    raise requests.RequestException(f"Failed to download {user}'s combined file. Status code: {response.status_code}")
        except Exception as e:
            print(f"An error occurred while processing {user}'s combined file: {e}")
        except requests.RequestException as e:
            print(f"Error occurred while fetching {user}'s combined file: {e}")

        if user in ["Kelly", "Swara", "Xinyu"]:
            irr_file_url = os.getenv(f"{user}_PARTNER_IRR_URL")
           
            try:
                if irr_file_url != "" and irr_file_url is not None:
                    response = requests.get(irr_file_url)
                    if response.status_code == 200:
                        with open(f"{getPartnerAlias(user)}_irr.xlsx", "wb") as f:
                            f.write(response.content)
                        table_file_to_csv(f"{getPartnerAlias(user)}_irr.xlsx", f"{file_dir}/{getPartnerAlias(user)}_irr.csv")
                        os.remove(f"{getPartnerAlias(user)}_irr.xlsx")
                        print(f"{user}'s irr file downloaded successfully.")
                    else:
                        raise requests.RequestException(f"Failed to download {user}'s irr file. Status code: {response.status_code}")
            except Exception as e:
                print(f"An error occurred while processing {user}'s irr file: {e}")
            except requests.RequestException as e:
                print(f"Error occurred while fetching {user}'s irr file: {e}")

if __name__ == "__main__":
    fetch_and_convert_files()