import requests
import json
import pandas as pd
import pymongo
import time
import random

proxies = {
    "http": "http://38.156.73.36:8085"
}

headers = {
    'Origin': 'https://www.medifind.com',
    'sec-ch-ua-platform': '"macOS"',
    'Referer': 'https://www.medifind.com/doctors/daniel-a-laheru/7443968',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    'sec-ch-ua-mobile': '?0'
}

client = pymongo.MongoClient("mongodb+srv://")
db = client["npi_directory"]
collection = db["medifind"]

def get_profile(person_id):
    profile_url = f"https://www.medifind.com/api/entity/doctor/{person_id}"
    profile_response = requests.request("GET", profile_url, headers=headers, proxies=proxies)
    print(f"profile code- {profile_response.status_code}")
    profile = json.loads(profile_response.text)
    return profile

def get_publications(person_id):
    pub_url = f"https://www.medifind.com/api/search/publications?personId={person_id}&recent=false&size=5000&page=1"
    payload = ""
    pub_response = requests.request("GET", pub_url, headers=headers, data=payload, proxies=proxies)
    publications = json.loads(pub_response.text)
    publications = publications["results"]
    return publications

def get_all_publications(person_id):
    page = 1
    all_publications = []

    while True:
        pub_url = f"https://www.medifind.com/api/search/publications?personId={person_id}&recent=false&size=10&page={page}"
        response = requests.get(pub_url, headers=headers, proxies=proxies)        
        if response.status_code != 200:
            print(f"Failed to fetch page {page}: {response.status_code}")
            break
        data = response.json()
        publications = data.get("results", [])
        if not publications:
            break
        all_publications.extend(publications)
        print(f"Collected---- {len(publications)} publications from page {page}")
        time.sleep(random.randint(5, 10))
        page += 1

    return all_publications

def get_clinical_trials(person_id):
    ct_url = "https://www.medifind.com/api/search/clinical-trials"

    payload = json.dumps({
    "personId": person_id,
    "noStatus": True
    })
    
    header = {
    'accept': '*/*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'authorization': 'null',
    'content-type': 'application/json',
    'origin': 'https://www.medifind.com',
    'priority': 'u=1, i',
    'referer': 'https://www.medifind.com/doctors/daniel-a-laheru/7443968',
    'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Cookie': '__gads=ID=d81c357af120d00e:T=1744032534:RT=1747217163:S=ALNI_MaUEYjR3-3C9MlIJPVbFMTBkLMXGA; __gpi=UID=0000108ee84de534:T=1744032534:RT=1747217163:S=ALNI_MYd0Z2bTZ0Z9P5lczg8IL_Up-NrcA; __eoi=ID=99081a815fac6322:T=1744032534:RT=1747217163:S=AA-AfjbLBL9t2NaDsCJPG_nDP6Ob'
    }

    ct_response = requests.request("POST", ct_url, headers=header, data=payload, proxies=proxies)
    print(f"clinical code- {ct_response.status_code}")

    clinical_trials = json.loads(ct_response.text)
    clinical_trials = clinical_trials["results"]
    return clinical_trials

def crawl(url):    
    # base_url = "https://www.medifind.com/doctors/daniel-a-laheru/7443968"
    person_id = url.rstrip('/').split('/')[-1]
    # pub_data = get_publications(person_id)
    pub_data = get_all_publications(person_id)
    # print(json.dumps(pub_data, indent=4))
    profil_data = get_profile(person_id)
    clinical_trials = get_clinical_trials(person_id)

    data = {
        "profile": profil_data,
        "publications": pub_data,
        "clinical_trials": clinical_trials
    }
    # with open(f'doctor_{person_id}.json', 'w', encoding='utf-8') as f:
    #     json.dump(data, f, ensure_ascii=False, indent=4)
    return data

def main():
    df = pd.read_csv("medifind_1.csv")
    data = json.loads(df.to_json(orient="records"))

    for idx, row in enumerate(data):
        url = row["url"]
        npi = row['npi']
        
        # collection.create_index("npi", unique=True)
        # collection.create_index("url", unique=True)

        is_exist = collection.find_one({"url": str(url)})    
            
        if is_exist is None:
            medifind_doctor_data = crawl(url)

            if medifind_doctor_data:
                _payload = {
                    "url": url,
                    "npi": npi,
                    "data": medifind_doctor_data
                }

                print(f"Dumping data for url: {url}, index= {idx}")
                print("\n")
                
                # print(json.dumps(_payload, indent=4))
                collection.insert_one(_payload)
                print(f"data uploaded for npi: {npi}, url: {url}")

                wai = random.randint(20, 30)
                time.sleep(wai)
                print(f"rest-time: {wai}")

if __name__ == "__main__":
    main()
