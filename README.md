# Twitter Stream Filter API

This set of APIs has been created to store twitter streaming data and retrieve data based on applied filters. It is a set of 3 APIs-
1. [API to trigger Twitter Stream](#1-api-to-trigger-twitter-stream-stream)
2. [API to filter/search stored tweets](#2-api-to-filtersearch-stored-tweets-search)
3. [API to export filtered data in CSV](#3-api-to-export-filtered-data-in-csv-getcsv)

Technologies used:
  - Python/ Flask framework
  - MongoDB (Hosted on MLab)
  - Twitter Streaming API
  
## Jump To
- [Installation Instructions](#installation-instructions)
- [API 1 - API to trigger Twitter Stream](#1-api-to-trigger-twitter-stream-stream)
- [API 2 - API to filter/search stored tweets](#2-api-to-filtersearch-stored-tweets-search)
- [API 3 - API to export filtered data in CSV](#3-api-to-export-filtered-data-in-csv-getcsv)
  
## Installation Instructions
  1. clone the project
  `git clone https://github.com/gauravkulkarni96/twitter-streaming-filter-api.git`
  2. cd to project folder `cd twitter-streaming-filter-api` and create virtual environment
  `virtualenv venv`
  3. activate virtual environment
  `source venv/bin/activate`
  4. install requirements
  `pip install -r requirements.txt`
  5. run the server
  `python run.py`
   
## 1. API to trigger Twitter Stream (/stream)
This API triggers twitter streaming and stores a curated version of the data returned by Twitter Streaming API. The streaming is done as per the given parameters.

API - `http://127.0.0.1:5000/stream/<keyword>?[parameters]`
(methods supported - GET, POST)

Where `<keyword>` can be any keyword for which streaming needs to be performed and `[parameters]` are as follows -
  
  | Parameter | Action |
  | ------ | ------ |
  | count | the streaming runs till given number of tweets are received |
  | time | the streaming runs for given time (seconds) |
  
  <b>Examples:</b>
  ```
  http://127.0.0.1:5000/stream/modi?count=5 (runs till 5 tweets are fetched)
  http://127.0.0.1:5000/stream/modi?time=10 (runs for 10 seconds)
  http://127.0.0.1:5000/stream/modi?count=5&time=10 (stops streaming whichever comes first i.e. 5 tweets or 10 seconds)
  ```
  ### API Response
  | Parameter | Meaning |
  | ------ | ------ |
  | code | 0 (successful)/ 1(failed) |
  | message | error message if api hit fails |
  | status | success/failed |
  
  <b>Examples:</b>
  
  1. Successful response
  ```
  {
    "code": "0",
    "message": "Successful",
    "status": "success"
  }
  ```
  2. Failed Response
  ```
  {
    "code": "1",
    "message": "No Parameters Passed",
    "status": "failed"
  }
  ```

## 2. API to filter/search stored tweets (/search)
This API fetches the data stored by the [first api](#1-api-to-trigger-twitter-stream) based on the filters and search keywords provided and sorts them as required.

API - `http://127.0.0.1:5000/search?[filters][sort][page]`
(methods supported - GET, POST)

<b>Following are the elements of the api:</b>
### Filters ([filters])
The filters follow format `<filter>=<value>` where `<filter>` can be one or more of filters mentioned below and `<value>` should be in the specified format. 
  
  
Following filters can be applied

  | Filter | Meaning | Value Format (refer table below) | Example |
  | ------ | ----- | ------ | ----- |
  | hashtag | filter tweets by hashtags in tweet (case insensitive) | `<hashtag>` | hashtag=AbKiBaarModiSarkar |
  | keyword | filter tweets by keyword which was used in API 1 for streaming | `<keyword>` | keyword=modi |
  | name | filter tweets by name/ screen_name of users (case insensitive) | `<textFilterType>-<filterValue>` | name=co-gaurav |
  | location | location of the user posting the tweet | `<location>` | location=delhi |
  | text | filter tweets by content (case insensitive) | `<textFilterType>-<filterValue>` | text=sw-gaurav |
  | type | filter tweets as retweets/quote/original tweets | original/retweet/quote | type=retweet |
  | mention | filter tweets by user mentions(case insensitive) | `<textFilterType>-<filterValue>` | mention=em-gauravkul96 |
  | followers | number of followers of the user | `<numFilterType><filterValue>`| followers=lt100 |
  | rtcount (mostly 0 in streaming) | retweet count of tweet | `<numFilterType><filterValue>`| rtcount=gt100 |
  | favcount (mostly 0 in streaming) | favourite count of tweet | `<numFilterType><filterValue>`| favcount=lt100 |
  | lang | Language of tweet | any specific language in [BCP 47](https://tools.ietf.org/html/bcp47) format | lang=en |
  | datestart | Tweets posted on or after a specific date | `dd-mm-yyyy` | datestart=10-01-2018 |
  | dateend | Tweets posted on or before a specific date | `dd-mm-yyyy` | dateend=28-02-2018 |
  
  In the format `<textFilterType><filterValue>`, `<filterValue>` can be any string and `<textFilterType>` can be

  | textFilterType | Meaning |
  | ------ | ------ |
  | sw | starts with |
  | ew | ends with |
  | co | contains |
  | em | exact match |
  
  In the format `<numFilterType><filterValue>`, `<filterValue>` can be any number and `<numFilterType>` can be

  | numFilterType | Meaning |
  | ------ | ------ |
  | gt | greater than |
  | lt | less than |
  | eq | equal to |
  | ge | greater than or equal to |
  | le | less than or equal to |
  
### Sort ([sort])
By default, sorting is done by date of tweet in descending order. Other sort types can be given by mentionin the `sort` parameter in the API in the format `<sortField>-<order>`

where  `<order>` can be

  | order | Meaning |
  | ------ | ----- |
  | asc | Ascending order |
  | dsc | descending order |
  
and `<sortField>` can be

  | sortField | Meaning | Example |
  | ------ | ------ | ------ |
  | name | sort by name | sort=name-asc |
  | sname | sort by screen name | sort=sname-dsc |
  | text | sort by tweet text | sort=text-asc |
  | fav | sort by favourites count | sort=fav-asc |
  | ret | sort by retweet count | sort=ret-dsc |
  | followers | sort by follower count of user | sort=followers-asc |
  | date | sort by date | sort=date-asc |
  
### Page ([page])
The API is paginated and returns 10 results in one call. The page number can be specified in the API call as `page=[pageNo]` for example `page=5`. Not speciftying the page number takes to page 1.

<b>Examples</b>
```
http://127.0.0.1:5000/search?favcount=lt1000&lang=en&datestart=10-01-2018&sort=date-asc
http://127.0.0.1:5000/search?name=co-gaurav&datestart=10-01-2018&dateend=15-01-2018&sort=text-asc&page=2
http://127.0.0.1:5000/search?rtcount=gt100
```

### API Response

  | Parameter | Meaning |
  | ------ | ------ |
  | page | current page number |
  | next_page | next page number (1 if current page is last page) |
  | last_page | Boolean true/false (true if current page is last page else false) |
  | result | list of tweet objects that match the given filters |
  | result_count | total number of matching results |
  
<b>Examples</b>
```
{
   "next_page": 1, 
   "last_page": true, 
   "result": [{"lang": "en", "_id": "5a83f5063fe5103329f1f788", "text": "RT @LalitKModi: Thank you #RichardMadley \ud83d\ude4f\ud83c\udffb most appreciative of your kind words  https://t.co/erkxF1q46i", "created_at": "2018-02-14 08:36:16+00:00", "hashtags": ["RichardMadley"], "retweet_count": 0, "user_mentions": ["LalitKModi"], "is_quote_status": false, "user": {"screen_name": "LaraeGalang3", "location": null, "_id": "5a83f5053fe5103329f1f786", "id": 963690891935473666, "name": "Larae Galang"}, "id": 963693359742312448, "favorite_count": 0, "is_retweet": true}],
   "page": 1,
   "result_count": 1
}

{
  "next_page": 3, 
  "last_page": false, 
  "result": [...], 
  "page": 2
  "result_count": 43
}
```

## 3. API to export filtered data in CSV (/getcsv)
This API returns the data in CSV. If opened in browser, it downloads a CSV file containin the data and if hit using another program, it returns the data in CSV format.

API : `http://127.0.0.1:5000/getcsv?[filters][sort]`
(methods supported - GET, POST)

`[filters]` and `[sort]` are the same parameters as defined in the [Second API](#2-api-to-filtersearch-stored-tweets-search) and there is no `[page]` parameter as all the matching data is returned.

<b>Examples</b>
```
http://127.0.0.1:5000/getcsv?hashtag=richardmadley
http://127.0.0.1:5000/getcsv?favcount=lt1000&lang=en&datestart=10-01-2018&sort=date-asc
```

### API Response
If the request to the API is sent using a browser, it downloads a CSV file containing data based on filters.
If the request is sent by another program/ application like Postman etc., the API returns the data in CSV format.
