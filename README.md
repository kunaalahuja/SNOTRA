# SNOTRA
codebase for SNOTRA: Scalable Neighbour based Online Textbook Recommendation Application 

to run the code, clone the repository and go the folder codebase: 

## Data Fetch
We use Goodreads API to fetch data for the book recommendation engine.
We expect you to have Goodreads API from here : https://www.goodreads.com/api/keys 
and it should be stored in 'key.txt'

[Usage]: python data_fetch \<operation\>

We support the following operations:
  
  i)   users - to get the list of users and their relevant information
  
  ii)  reviews - get the reviews details
  
  iii) book_id - get all the book ids
  
  iv)  books - get the book details
  
  v)   csv - write all the csvs needed for the recommendation engine
  
## Application
[Usage]: python application.py

Open http://127.0.0.1:5000/ to view the SNOTRA webapp.
