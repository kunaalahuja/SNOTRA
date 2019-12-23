import xmltodict
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import random
import collections
import sys

#GLOBAL COUNTERS

TOTAL_USERS = 103525406
SAMPLE_USERS = 100000
USERS_COUNT = 12000
#SAMPLE_USERS = 100
#USERS_COUNT = 5

#FILE NAMES
USER_FILE = 'users.csv'

def get_goodreads_api_key():
    try:
        f = open("key.txt", "r")
        key = f.readline()
        f.close()    
        return key

    except OSError:
        print('[ERROR]:Please provide your GoodReads API key in key.txt')
    
def write_to_csv(df, file):
    df.to_csv(file)   
    print("### Done writing to file : ", file)

def get_soup(url):
    r = requests.get(url, timeout = 5)      
    soup = BeautifulSoup(r.text, "lxml")
    
    return soup

def get_xml_data(url):
    r = requests.get(url, timeout = 5)
    xml_data = xmltodict.parse(r.content)
      
    return xml_data

def get_user_data(userIDs, key):
    #create template dataframe to hold the temporary results
    df = pd.DataFrame(columns = ['user_id','user_reviews_count'])
    exception_list = []

    user_ctr = 0
    for userID in userIDs:
        try:
            data_url = 'https://www.goodreads.com/user/show/' + str(userID) +'.xml?key=' + key
            soup = get_soup(data_url)
            
            if soup.html.body.error is not None:
                continue
                
            if soup.html.user.id is not None:
                if(len(soup.html.user.id.contents)>0):
                    df.loc[user_ctr,'user_id'] = soup.html.user.id.contents[0]
                    
            if soup.html.user.user_shelves is not None:
                if(len(soup.html.user.user_shelves.contents)>0):
                    for element in soup.html.user.user_shelves.find_all("user_shelf"):
                        if(element.find_all("name")[0].contents[0] == 'read' ):
                            total_reviews = int(element.book_count.contents[0])
                            df.loc[user_ctr,'user_reviews_count'] = total_reviews
                            break
            user_ctr += 1
        
        except:
            exception_list.append(userID)
            continue

    # Selecting a subset of 12000 users who have atleast 1 review    
    df = df.dropna()
    df = df[df['user_reviews_count']!=0]    
    df = df[:USERS_COUNT]
    df.reset_index(drop=True, inplace=True)
    
    return df

def add_username(user_df, key):    
    user_df['name'] = ''
    exception_list = []

    for j in range(len(user_df)):
        try:
            data_url = 'https://www.goodreads.com/user/show/' + str(user_df.loc[j,'user_id']) +'.xml?key=' + key
            data = get_xml_data(data_url)
            user_df.loc[j,'name'] = data['GoodreadsResponse']['user']['name']

        except:
            exception_list.append(j)
            continue

    return user_df        

def write_user_file():
    key = get_goodreads_api_key()
    
    # Randomly select a list of 100000 users
    random_users = random.sample(range(0, TOTAL_USERS), SAMPLE_USERS)
    user_df = get_user_data(random_users, key)

    #TODO: merge the add username logic to get_user_data <Kunaal>
    user_df = add_username(user_df, key)

    write_to_csv(user_df, USER_FILE)

if __name__ == "__main__":
    operation = str(sys.argv[1])
    if operation == "users":
        write_user_file()