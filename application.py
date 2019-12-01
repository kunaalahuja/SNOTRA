# Importing libraries
from flask import Flask, flash, redirect, render_template, request, session, abort,send_from_directory,send_file,jsonify
import pandas as pd
import pandas as pd
import numpy as np
import json
from IPython.display import display, Markdown
import warnings

warnings.filterwarnings('ignore')

##############################################################
##
## Data Preparation
##
##############################################################
                    
#reviews_filename = "users_and_their_ratings_sam.csv"
reviews_filename = "user_reviews_1.csv"
books_filename = "books_with_details_Kunaal.csv"

# Reading data from the CSV
def get_data():
  ratings_data_raw = pd.read_csv(reviews_filename)
  return ratings_data_raw

def get_clean_data():
  ratings_data_raw = get_data()
  ratings_data = data_cleanup(ratings_data_raw)
  return ratings_data

#Data cleanup
def data_cleanup(ratings_data_raw):
  #remove nulls and 0 entry rows
  ratings_data = ratings_data_raw[(ratings_data_raw['Rating'] != 0) & (ratings_data_raw['Rating'] != '')]
  
  #remove unnecessary columns
  # del ratings_data['Unnamed: 0']
  del ratings_data['Book_name']
  #del ratings_data['average_rating']
  del ratings_data['Image_url']

  #rename columns
  ratings_data.rename(columns = {'Rating':'rating', 'User_id':'user_id', 'Book_id':'book_id'}, inplace = True) 
  return ratings_data


##############################################################
##
## Collaborative Filtering
##
##############################################################

#Build the tuples of ratings, book_id and user_id
def buildMatrix(df):
    df = df.pivot(index='user_id', columns='book_id', values='rating')
    df = df.fillna(0)

    user_id_index_temp = list(df.index)
    user_id_index = {}
    
    for count, each in enumerate(user_id_index_temp, 0):
        user_id_index[int(each)] = count

    index_book_id_temp = list(df.columns)
    index_book_id = {}

    for count, each in enumerate(index_book_id_temp, 0):
        index_book_id[count] = int(each)

    return (df, user_id_index, index_book_id)

#this is where magic happens, total crazzyyyy
def cache_matrix(reInitialize = False, fetchDataFromCsv = True):
    if reInitialize or 'ratings_matrix' not in globals():
      global ratings_matrix 
      global user_id_index
      global index_book_id
      global ratings_data

      if fetchDataFromCsv == True:
        ratings_data_raw = get_data()
        ratings_data = data_cleanup(ratings_data_raw)

      user_rating_matrix = buildMatrix(ratings_data)
      ratings_matrix = user_rating_matrix[0]
      user_id_index = user_rating_matrix[1]
      index_book_id = user_rating_matrix[2]

#Calculate the cosine similarity metric
def calc_cosine_similarity(vA,vB):
    sumA = np.sqrt(np.sum(np.multiply(vA,vA)))
    sumB = np.sqrt(np.sum(np.multiply(vB,vB)))
    sumAB = np.sum(np.multiply(vA,vB))
    cosine_similarity = sumAB / (sumA * sumB)

    return cosine_similarity

#Get similar users for a given user
def get_similar_users(ratings, user_id_index, current_user_id, topN):
    selected_row = np.array(ratings[ratings.index == current_user_id])
    user_dict = {}

    for index, row in ratings.iterrows():
        if(index != current_user_id):
            user_dict[index] = calc_cosine_similarity(selected_row,np.array(row))            
    
    user_dict = sorted(user_dict.items(), key=lambda x:x[1], reverse=True)
    user_dict = user_dict[:topN]
    
    top_user_ids = [i for i,j in user_dict]

    return top_user_ids

#Gets the recommended books from a similar user
def recommend_book_from_sim_user(ratings, similar_user_ids, user_id_index, index_book_id, topN):
  books = []  
  for user_id in similar_user_ids:
      selected_book_ratings = ratings_matrix.loc[user_id]
      selected_book_ratings.sort_values(ascending=False, inplace = True)
      books.extend(list(selected_book_ratings[:topN].index))

  return books

#this is the exposed end point, returns the recommended books for a given user
def get_recommended_books(current_user_id, reInitialize = False, topN = 5, topBooks = 1):
  cache_matrix(reInitialize)
  similar_users = get_similar_users(ratings_matrix, user_id_index, current_user_id, topN)
  return recommend_book_from_sim_user(ratings_matrix, similar_users, user_id_index, index_book_id, topBooks)

##############################################################
##
## Helper Functions
##
##############################################################

#creates a map of book_id to image_url
def get_book_url_map():
  reviews_raw = pd.read_csv(reviews_filename)
  book_url_df = reviews_raw[['Book_id', 'Image_url']]
  book_url_df.rename(columns = {'Image_url':'image_url', 'Book_id':'book_id'}, inplace = True) 
  return book_url_df.set_index('book_id').T.to_dict()

#creates a map of book_id to book_name
def get_book_name_map():
  reviews_raw = pd.read_csv(reviews_filename)
  book_url_df = reviews_raw[['Book_id', 'Book_name']]
  book_url_df.rename(columns = {'Book_name':'book_name', 'Book_id':'book_id'}, inplace = True) 
  return book_url_df.set_index('book_id').T.to_dict()

#returns the avarage rating of a book
def get_average_book_rating():
  reviews_raw = pd.read_csv(reviews_filename)
  book_url_df = reviews_raw[['Book_id', 'Rating']]
  book_url_df.rename(columns = {'Rating':'rating', 'Book_id':'book_id'}, inplace = True) 
  return book_url_df.groupby('book_id').mean()

#Get n random books to show a new user 
def get_random_books(n=10): 
  books_raw = pd.read_csv(books_filename)
  book_df = books_raw[['book_id','author', 'book_url', 'book_name', 'genre', 'image_url', 'average_rating']]
  book_df.rename(columns = {'book_name':'book_name_long'}, inplace = True) 
  book_df['book_id_copy'] = book_df['book_id']
  book_df['book_name'] = book_df['book_name_long'].astype(str).str[:30]
  book_df['author'] = book_df['author'].astype(str).str[:30]
        
  book_df = book_df.sample(n=n)
  return book_df.set_index('book_id_copy').T.to_dict()


#Crazzzzy max work! I like the design here. I like, I like, I like!
def get_custom_userID(random_books, rating1, rating2, rating3, rating4, rating5, rating6, rating7, rating8, rating9, rating10):
  global ratings_data
  new_user_id = int(ratings_data[['user_id']].max().user_id + 1)

  random_books = pd.DataFrame(random_books).T.reset_index()
  
  new_data = list()
  new_data.append({"book_id":random_books.iloc[0].book_id, "rating":int(rating1), "user_id":new_user_id})
  new_data.append({"book_id":random_books.iloc[1].book_id, "rating":int(rating2), "user_id":new_user_id})
  new_data.append({"book_id":random_books.iloc[2].book_id, "rating":int(rating3), "user_id":new_user_id})
  new_data.append({"book_id":random_books.iloc[3].book_id, "rating":int(rating4), "user_id":new_user_id})
  new_data.append({"book_id":random_books.iloc[4].book_id, "rating":int(rating5), "user_id":new_user_id})
  new_data.append({"book_id":random_books.iloc[5].book_id, "rating":int(rating6), "user_id":new_user_id})
  new_data.append({"book_id":random_books.iloc[6].book_id, "rating":int(rating7), "user_id":new_user_id})
  new_data.append({"book_id":random_books.iloc[7].book_id, "rating":int(rating8), "user_id":new_user_id})
  new_data.append({"book_id":random_books.iloc[8].book_id, "rating":int(rating9), "user_id":new_user_id})
  new_data.append({"book_id":random_books.iloc[9].book_id, "rating":int(rating10), "user_id":new_user_id})
  new_df = pd.DataFrame(new_data)

  ratings_data = pd.concat([ratings_data, new_df])
  cache_matrix(reInitialize = True, fetchDataFromCsv = False)

  return new_user_id

#gets all the information we've for each book  
def get_book_info_map():
  books_raw = pd.read_csv(books_filename)
  book_info_df = books_raw[['book_id', 'author', 'book_url', 'book_name', 'genre', 'image_url', 'average_rating']]
  #book_info_df["image_url"] = book_info_df["book_url"].str.replace("/show/", "/photo/")
  return book_info_df.set_index('book_id').T.to_dict()


#############################################################
##
## FLASK
##
#############################################################

application= Flask(__name__)

class DataStore():
    UserID=None
    
data=DataStore()

@application.route("/main",methods=["GET","POST"])

#3. Define main code
@application.route("/",methods=["GET","POST"])

def homepage():
    if 'ratings_data' not in globals():
      global ratings_data
      ratings_data = get_clean_data()
    
    random_books = get_random_books()
    default_user_id = 16291939

    UserId   = request.form.get('UserId', default_user_id)
  
    rating1  = rating2 = rating3 = rating4 = rating5 = rating6 = rating7 = rating8 = rating9 = rating10 = 0
    rating1  = request.form.get('Rating1',  0)
    rating2  = request.form.get('Rating2',  0)
    rating3  = request.form.get('Rating3',  0)
    rating4  = request.form.get('Rating4',  0)
    rating5  = request.form.get('Rating5',  0)
    rating6  = request.form.get('Rating6',  0)
    rating7  = request.form.get('Rating7',  0)
    rating8  = request.form.get('Rating8',  0)
    rating9  = request.form.get('Rating9',  0)
    rating10 = request.form.get('Rating10', 0)

    if(rating1 != 0 or rating2 != 0 or rating3 != 0 or rating4 != 0 or rating5 != 0 or rating6 != 0 or rating7 != 0 or rating8 != 0 or rating9 != 0 or rating10 != 0):    
        print("here 2")
        UserId = get_custom_userID(random_books, rating1, rating2, rating3, rating4, rating5, rating6, rating7, rating8, rating9, rating10)
        print("new userID", UserId)
    #print(UserId)
        
    data.UserId = UserId
    print("new userID", UserId)
    books = get_recommended_books(int(UserId)) 
    print("Userid: ",UserId, " -- ", books )

    books_info_map = get_book_info_map()

    books = [ [book] for book in books]
    df = pd.DataFrame(books,  columns = ['BookID'])
    flare = dict()

    d = {"name": "flare", "children": [], "random_books": random_books}
    
    for row in df.values:
        userID = str(UserId)
        bookID = str(row[0])

        ##Get Book information for displaying
        book_name  = books_info_map[row[0]]['book_name'][:30]
        image_url  = books_info_map[row[0]]['image_url']
        author     = books_info_map[row[0]]['author'][:30]
        genre      = books_info_map[row[0]]['genre']
        avg_rating = books_info_map[row[0]]['average_rating']
        book_url   = books_info_map[row[0]]['book_url']

        # make a list of keys
        keys_list = []
        for item in d['children']:
            keys_list.append(item['userID'])

        books_info = {
            "bookID"    : bookID,
            "book_name" : book_name,
            "book_url"  : book_url,
            "image_url" : image_url,
            "author"    : author,
            "genre"     : genre,
            "avg_rating": avg_rating
        }    

        # if 'the_parent' is NOT a key in the flare.json yet, append it
        if not userID in keys_list:
            d['children'].append({"userID": userID, "children": [books_info]})

        # if 'the_parent' IS a key in the flare.json, add a new child to it
        else:
            d['children'][keys_list.index(userID)]['children'].append(books_info)

    flare = d
    e = json.dumps(flare)

    data.Prod = json.loads(e)
    Prod = data.Prod

    return render_template("index.html", Prod=Prod)

@application.route("/get-data", methods=[ "GET", "POST" ])
def returnProdData():
    f = data.Prod
    return jsonify(f)
# export the final result to a json file

if __name__ == "__main__":
    application.run( debug = True )
