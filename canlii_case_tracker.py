import pandas as pd
from obiter.canlii_api import *
import os
from datetime import datetime
import requests
import time
import sys

# Define the base directory as the home directory
BASE_DIR = os.path.expanduser('~')

# pushover
api_token = os.getenv("PUSHOVER_API_TOKEN")
user_key = os.getenv("PUSHOVER_USER_KEY")
pushover_url = "https://api.pushover.net/1/messages.json"

def send_notification(message, api_token=api_token, user_key=user_key, pushover_url=pushover_url):
    data = {
        "token": api_token,
        "user": user_key,
        "message": message,
    }

    response = requests.post(pushover_url, data=data)
    if response.status_code != 200:
        sys.exit('Failed to send notification. Exiting script.')

# send a notification to say that the script has started
message = f'Started the case list scraper script at {datetime.now()}'
send_notification(message)

language = 'en'  # or 'fr'
APIkey = os.environ['CANLII_API_KEY']
api_caller = canlii_api(APIkey, language)

today = datetime.today()
today = today.strftime('%Y-%m-%d')
year = today.split('-')[0]

def fetch_cases_with_retries(language, databaseId, APIkey, resultCount, offset=0, max_retries=10):
    url = f'https://api.canlii.org/v1/caseBrowse/{language}/{databaseId}/?&api_key={APIkey}&resultCount={resultCount}&offset={offset}'
    
    for attempt in range(max_retries):
        try:
            res = requests.get(url)
            res.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
            df = pd.DataFrame(res.json()['cases'])
            return df
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(1)  # Optional: Wait for a second before retrying
        except KeyError as e:
            print(f"Key error: {e}")
            break

    send_notification(f"Script aborted because max_retries reached.")
    sys.exit("Max retries reached. Exiting script.")

# check if there is a tribunals history folder
if not os.path.exists(os.path.join(BASE_DIR, 'tribunals')):
    os.mkdir(os.path.join(BASE_DIR, 'tribunals'))
    
# check if there is a sub folder for history
if not os.path.exists(os.path.join(BASE_DIR, 'tribunals/list_history')):
    os.mkdir(os.path.join(BASE_DIR, 'tribunals/list_history'))
    
# check if there is a daily tracker folder
if not os.path.exists(os.path.join(BASE_DIR, 'daily_tracker')):
    os.mkdir(os.path.join(BASE_DIR, 'daily_tracker'))

# check if there is a tribunals csv file
if not os.path.exists(os.path.join(BASE_DIR, 'tribunals.csv')):
    tribunals = api_caller.list_tribunals()
    tribunals.to_csv(os.path.join(BASE_DIR, '/tribunals/tribunals.csv'), index=False)
    tribunals.to_csv(os.path.join(BASE_DIR, f'tribunals/list_history/tribunals_{today}.csv'), index=False)
    print('No tribunals file found. Created one.')
else:
    # load the csv file
    tribunals = pd.read_csv(os.path.join(BASE_DIR, 'tribunals.csv'))
    # get its size
    number_of_tribunals = tribunals.shape[0]

    # check for today's list of tribunals
    today_tribunals = api_caller.list_tribunals()
    # get its size
    number_of_today_tribunals = today_tribunals.shape[0]
    
    # check if the number of tribunals has changed
    if number_of_tribunals != number_of_today_tribunals:
        tribunals = today_tribunals
        tribunals.to_csv(os.path.join(BASE_DIR, 'tribunals.csv'), index=False)
        tribunals.to_csv(os.path.join(BASE_DIR, f'tribunals/list_history/tribunals_{today}.csv'), index=False)
        print(f'Tribunal list updated. Old list had {number_of_tribunals} tribunals. New list has {number_of_today_tribunals} tribunals.')
        send_notification(f'Tribunal list updated. Old list had {number_of_tribunals} tribunals. New list has {number_of_today_tribunals} tribunals.')
    else:
        print('Tribunal list is up to date.')

# iterate through the tribunals and get lists of all the decisions

resultCount = 10000  # number of results
daily_tracker = pd.DataFrame()

for idx, row in tribunals.iterrows():
    databaseId = row['databaseId']
    jurisdiction = row['jurisdiction']
    tribunal = row['name']
    
    # check if there is a jurisdiction folder
    if not os.path.exists(os.path.join(BASE_DIR, f'tribunals/{jurisdiction}')):
        os.mkdir(os.path.join(BASE_DIR, f'tribunals/{jurisdiction}'))
    
    # check if there is a tribunal folder within the jurisdiction folder
    if not os.path.exists(os.path.join(BASE_DIR, f'tribunals/{jurisdiction}/{tribunal}')):
        os.mkdir(os.path.join(BASE_DIR, f'tribunals/{jurisdiction}/{tribunal}'))
        
    # check if there is a caselist csv file
    if os.path.exists(os.path.join(BASE_DIR, f'tribunals/{jurisdiction}/{tribunal}/caselist.csv')):
        caselist = pd.read_csv(os.path.join(BASE_DIR, f'tribunals/{jurisdiction}/{tribunal}/caselist.csv'))
        print(f'Loaded caselist for {tribunal}, {jurisdiction}.')
        
        original_length = len(caselist)
        
        temp = fetch_cases_with_retries(language, databaseId, APIkey, resultCount, offset=0, max_retries=10)
        temp['scrape_date'] = today
        
        caselist = pd.concat([caselist, temp], ignore_index=True).reset_index(drop=True)
        caselist = caselist.drop_duplicates(subset=['citation'], keep='first')
        
        new_length = len(caselist)
        difference = new_length - original_length
        
        caselist.to_csv(os.path.join(BASE_DIR, f'tribunals/{jurisdiction}/{tribunal}/caselist.csv'), index=False)
        print(f'Updated caselist for {tribunal}, {jurisdiction}. It had {original_length} cases. Now it has {new_length} cases. An addition of {difference} cases.')
        
        daily_tracker.loc[idx, 'tribunal'] = tribunal
        daily_tracker.loc[idx, 'jurisdiction'] = jurisdiction
        daily_tracker.loc[idx, 'original_length'] = original_length
        daily_tracker.loc[idx, 'new_length'] = new_length
        daily_tracker.loc[idx, 'difference'] = difference
        
    else:
        caselist = fetch_cases_with_retries(language, databaseId, APIkey, resultCount, offset=0, max_retries=10)
        caselist['scrape_date'] = today
        if len(caselist) == 0:
            print(f'No cases found for {tribunal}, {jurisdiction}.')
            pass
        elif len(caselist) < 10000:
            caselist.to_csv(os.path.join(BASE_DIR, f'tribunals/{jurisdiction}/{tribunal}/caselist.csv'), index=False)
            print(f'Created caselist for {tribunal}, {jurisdiction}. It has {len(caselist)} cases.')
        else:
            flag = True
            while flag == True:
                last_value = caselist.index[-1]
                temp = fetch_cases_with_retries(language, databaseId, APIkey, resultCount, offset=(last_value + 1), max_retries=10)
                temp['scrape_date'] = today
                caselist = pd.concat([caselist, temp], ignore_index=True).reset_index(drop=True)
                if len(temp) < 10000:
                    flag = False
            caselist = caselist.drop_duplicates(subset=['citation'], keep='first')
            caselist.to_csv(os.path.join(BASE_DIR, f'tribunals/{jurisdiction}/{tribunal}/caselist.csv'), index=False)
            print(f'Created caselist for {tribunal}, {jurisdiction}. It has {len(caselist)} cases.')

daily_tracker = daily_tracker.fillna(0)
daily_tracker.to_csv(os.path.join(BASE_DIR, f'daily_tracker_{today}.csv'), index=False)

# send the update message

message = f"""Good morning,

{int(daily_tracker['difference'].sum())} cases were added to CanLii.

Here are the breakdowns:
"""

daily_tracker = daily_tracker.sort_values(by='difference', ascending=False)
for idx, row in daily_tracker[daily_tracker['difference'] > 0].iterrows():
    text = f"{row['tribunal']}, ({row['jurisdiction']}): {int(row['difference'])} cases added.\n\n"
    message += text

send_notification(message)