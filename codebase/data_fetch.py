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
#SAMPLE_USERS = 100000
#USERS_COUNT = 12000
SAMPLE_USERS = 100
USERS_COUNT = 5

#FILE NAMES
USERS_FILE        = 'users.csv'
REVIEWS_FILE      = 'reviews.csv'
BOOKS_FILE        = 'books.csv'
BOOKS_ID_FILE     = 'books_id.csv'
DATA_FILE         = 'master_data.csv'
BOOKS_DETAIL_FILE = 'books_details.csv'
BOOKS_DESC_FILE   = 'books_desc.csv'
USER_RATINGS_FILE = 'user_ratings.csv'


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

    write_to_csv(user_df, USERS_FILE)

def get_book_ids_df():
    df = pd.read_csv(BOOKS_ID_FILE)
    df['genre'] = ''
    df['book_url'] = ''
    df['average_rating'] = 0.0
    df['author'] = ''
    df.drop_duplicates(inplace=True)
    df = df.reset_index(drop=True)

    return df

def fetch_book_details():
    key = get_goodreads_api_key()
    df  = get_book_ids_data()    
    exception_list = []

    for i in range(len(df)):
        try:
            data_url = 'https://www.goodreads.com/book/show/'+ str(df.loc[i,'book_id']) +'.xml?key=' + key
            data = get_xml_data(data_url)
            
            # Fetching url and average rating 
            df.loc[i,'book_url'] = 'https://www.goodreads.com/book/show/'+str(df.loc[i,'book_id'])
            df.loc[i,'average_rating'] = float(data['GoodreadsResponse']['book']['average_rating'])
            
            # Fetching author
            if(type(data['GoodreadsResponse']['book']['authors']['author']) is not list):
                df.loc[i,'author'] = data['GoodreadsResponse']['book']['authors']['author']['name']
            else:
                df.loc[i,'author'] = data['GoodreadsResponse']['book']['authors']['author'][0]['name']
            
            # Fetching genre
            no_of_shelves = len(data['GoodreadsResponse']['book']['popular_shelves']['shelf'])
            for j in range(no_of_shelves):
                genre = data['GoodreadsResponse']['book']['popular_shelves']['shelf'][j]['@name']
                if genre!='to-read' and genre!='currently-reading' and genre!='favorites':
                    break            
            df.loc[i,'genre'] = genre
        
        except:
            exception_list.append(i)

    write_to_csv(df, BOOKS_FILE)            

def create_csv_files():
    #REVIEWS
    reviews_df = pd.read_csv(REVIEWS_FILE)
    reviews_df['user_id'] = reviews_df['user_id'].apply(int)
    reviews_df.drop(columns = ['review description', 'author'], inplace=True)

    books_df = pd.read_csv(BOOKS_FILE, encoding='iso-8859-1')
    users_df = pd.read_csv(USERS_FILE, encoding='iso-8859-1')

    books_data_df = pd.merge(reviews_df, books_df, how='left', on=['book_id'])
    # Master data including all fields that we care about
    master_df = pd.merge(books_data_df, users_df, how='left', on=['user_id'])
    
    write_to_csv(master_df, DATA_FILE)
        
    # Creating subsets of data relevant to different team members
    book_details_df = books_data_df[['book_id','book_name','image_url','genre','book_url','average_rating','author']]
    book_details_df.drop_duplicates(inplace=True)
    write_to_csv(book_details_df, BOOKS_DETAIL_FILE)

    book_desc_df = books_data_df[['book_description','book_id','book_name','average_rating']]
    book_desc_df.drop_duplicates(inplace=True)
    write_to_csv(book_desc_df, BOOK_DESC_FILE)
    
    user_ratings_df = books_data_df[['book_id','book_name','rating','user_id','average_rating']]
    user_ratings_df.drop_duplicates(inplace=True)
    write_to_csv(user_ratings_df, USER_RATINGS_FILE)
    
if __name__ == "__main__":
    operation = str(sys.argv[1])
    if operation == "users":
        write_user_file()
    elif operation == "csv":
        create_csv_files()
    else:
        print("[USAGE ERROR]: Choose one of the following operations: users, csv")    