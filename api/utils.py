from . import app
import json
from datetime import datetime
from bson import ObjectId
from tweepy.streaming import StreamListener
#MongoDB
from flask_pymongo import PyMongo
import pymongo

mongo = PyMongo(app)

# Custom JSON converter to support Datetime and Object types
class JSONEncoder(json.JSONEncoder):
	def default(self, o):
		if isinstance(o, ObjectId):
			return str(o)
		if isinstance(o, datetime):
			return str(o)
		return json.JSONEncoder.default(self, o)


def storeData(data, keyword):
	"""
	Function to store curated data in database
	"""
	users = mongo.db.users
	tweets = mongo.db.tweets
	duplicate_tweet = tweets.find_one({'id':data['id']})
	if duplicate_tweet == None:
		user_keys = ['id','screen_name', 'name', 'location', 'followers_count']
		qtest = users.find_one({'id':data['user']['id']})
		if qtest == None:
			saveuser = {key: data['user'][key] for key in user_keys}
			saveuser["name_lower"] = data['user']['name'].lower()
			saveuser["screen_name_lower"] = saveuser["screen_name"].lower()
			if data['user']['location']:
				saveuser["location_lower"] = data['user']['location'].lower()
			users.insert(saveuser)

		## For storing mentions in users database
		## Better data structure but makes filtering slower 
		
		# for x in data['entities']['user_mentions']:
		# 	qtest = users.find_one({'id':x['id']})
		# 	if qtest == None:
		# 		saveuser = {
		# 			'id':x['id'],
		# 			'screen_name': x['screen_name'],
		# 			'screen_name_lower': x['screen_name'].lower(),
		# 			'name': x['name'],							 
		# 			'name_lower': x['name'].lower(),
		# 		}
		# 		users.insert(saveuser)

		data['user'] = data['user']['id']

		data_keys = ['favorite_count', 'id', 'is_quote_status', 'lang', 'retweet_count','user']
		save_data = {key: data[key] for key in data_keys}

		if data['truncated'] and 'extended_tweet' in data and 'full_text' in data['extended_tweet']:
			save_data['text'] = data['extended_tweet']['full_text']
		else:
			save_data['text'] = data['text']

		save_data['created_at'] = datetime.strptime(data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
		save_data['text_lower'] = save_data['text'].lower()
		save_data['hashtags'] = [x['text'] for x in data['entities']['hashtags']]
		save_data['hashtags_lower'] = [x['text'].lower() for x in data['entities']['hashtags']]
		save_data['user_mentions'] = [x['screen_name'] for x in data['entities']['user_mentions']]
		save_data['user_mentions_lower'] = [x['screen_name'].lower() for x in data['entities']['user_mentions']]
		save_data['keyword'] = keyword.lower()

		save_data['is_retweet'] = False
		if 'retweeted_status' in data:
			save_data['is_retweet'] = True

		tweets.insert(save_data)


class StdOutListener(StreamListener):
	"""
	StreamListener class for working on streaming tweets
	"""
	def __init__(self, time, count, keyword):
		self.max_tweet = count
		self.time = time
		self.tweet_count = 0
		self.start_time = datetime.now()
		self.keyword = keyword

	def on_data(self, data):
		if self.time and (datetime.now()-self.start_time).seconds >= self.time:
			return False

		dtemp = json.loads(data)
		storeData(dtemp, self.keyword)

		self.tweet_count+=1
		if self.time and (datetime.now()-self.start_time).seconds >= self.time:
			return False

		if self.max_tweet and self.tweet_count >= self.max_tweet:
			return False

		return True

	def on_error(self, status):
		print status


def filterData(name, text, retweet_count, favourite_count, date_start_string, date_end_string, 
				language, mention, sort_param, hashtag, user_follow_count, 
				tweet_type, location, keyword):
	"""
	Function to filter data based on applied filters
	"""
	users = mongo.db.users
	tweets = mongo.db.tweets

	primary_sort, primary_sort_type = "created_at", pymongo.DESCENDING
	secondary_sort=None
	secondary_sort_type = None
	if sort_param != None and '-' in sort_param:
		secondary_sort_temporary, secondary_sort_type_temporary = sort_param.strip().split("-")
		if secondary_sort_temporary and secondary_sort_type_temporary:
			if secondary_sort_temporary == "name":
				secondary_sort = "user"
			elif secondary_sort_temporary == "sname":
				secondary_sort = "screen_name"
			elif secondary_sort_temporary == "followers":
				secondary_sort = "followers_count"

			elif secondary_sort_temporary == "text":
				primary_sort = "text"
			elif secondary_sort_temporary == "fav":
				primary_sort = "favorite_count"
			elif secondary_sort_temporary == "ret":
				primary_sort = "retweet_count"
			else:
				primary_sort = "created_at"

			if secondary_sort_type_temporary == "asc":
				if secondary_sort:
					secondary_sort_type="asc"
				else:
					primary_sort_type = pymongo.ASCENDING
			elif secondary_sort_type_temporary == "dsc":
				if secondary_sort:
					secondary_sort_type="dsc"
				else:
					primary_sort_type = pymongo.DESCENDING
		else:
			primary_sort, primary_sort_type = "created_at", pymongo.DESCENDING

	user_filters_dictionary = {}
	basic_filters_dictionary = {}

	if keyword != None:
		basic_filters_dictionary["keyword"] = keyword.lower()

	if tweet_type != None:
		if tweet_type == "retweet":
			basic_filters_dictionary["is_retweet"] = True
		elif tweet_type == "quote":
			basic_filters_dictionary["is_quote_status"] = True
		elif tweet_type == "original":
			basic_filters_dictionary["is_retweet"] = False
			basic_filters_dictionary["is_quote_status"] = False

	if hashtag != None:
		basic_filters_dictionary['hashtags_lower'] = hashtag.lower()

	if date_start_string != None and date_end_string == None and len(date_start_string)==10:
		# try:
			date, month, year = map(int, date_start_string.strip().split("-"))
			date_start = datetime(year, month, date)
			basic_filters_dictionary['created_at'] = {'$gte':date_start}
		# except:
		# 	pass

	if date_end_string != None and date_start_string == None and len(date_end_string)==10:
		# try:
			date, month, year = map(int, date_end_string.strip().split("-"))
			date_end = datetime(year, month, date)
			basic_filters_dictionary['created_at'] = {'$lte':date_end}
		# except:
		# 	pass

	if date_start_string != None and date_end_string != None and len(date_start_string)==10 and len(date_end_string)==10:
		# try:
			date, month, year = map(int, date_start_string.strip().split("-"))
			date_start = datetime(year, month, date)

			date, month, year = map(int, date_end_string.strip().split("-"))
			date_end = datetime(year, month, date)
			basic_filters_dictionary['$and'] = [
									{'created_at':{'$gte':date_start}},
									{'created_at':{'$lte':date_end}}
								 ]
		# except:
		# 	pass

	if location != None:
		user_filters_dictionary["location_lower"] = location.lower()

	if user_follow_count != None:
		ufollowcountType = user_follow_count[:2].lower()
		user_follow_count = user_follow_count[2:]
		if user_follow_count.isdigit():
			if ufollowcountType == "gt":
				user_filters_dictionary["followers_count"] = {'$gt':int(user_follow_count)}
			elif ufollowcountType == "lt":
				user_filters_dictionary["followers_count"] = {'$lt':int(user_follow_count)}
			elif ufollowcountType == "eq":
				user_filters_dictionary["followers_count"] = {'$eq':int(user_follow_count)}
			elif ufollowcountType == "le":
				user_filters_dictionary["followers_count"] = {'$lte':int(user_follow_count)}
			elif ufollowcountType == "ge":
				user_filters_dictionary["followers_count"] = {'$gte':int(user_follow_count)}
	
	if name != None:
		name_match_type = name[:2].lower()
		name = name[3:].lower()

		if name_match_type == "sw":
			user_filters_dictionary["$or"] = [
						{"name_lower" : {'$regex' : "^"+name.lower()}}, 
						{"screen_name_lower" : {'$regex' : "^"+name.lower()}}
					]

		elif name_match_type == "ew":
			user_filters_dictionary["$or"] = [
				{"name_lower" : {'$regex' : name.lower()+"$"}}, 
				{"screen_name_lower" : {'$regex' : name.lower()+"$"}}
			]

		elif name_match_type == "co":
			user_filters_dictionary["$or"] = [
				{"name_lower" : {'$regex' : name.lower()}}, 
				{"screen_name_lower" : {'$regex' : name.lower()}}
			]

		elif name_match_type == "em":
			user_filters_dictionary["$or"] = [
				{"name_lower" : name.lower()}, 
				{"screen_name_lower" : name.lower()}
			]
		
	if name != None or user_follow_count != None or location != None:
		nameids = []
		for i in users.find(user_filters_dictionary):
			nameids.append(i['id'])
		basic_filters_dictionary['user'] = {'$in' : nameids}

	if mention != None:
		mention_match_type = mention[:2].lower()
		mention = mention[3:].lower()
		if mention_match_type == "sw":
			basic_filters_dictionary["user_mentions_lower"] = {'$regex' : "^"+mention.lower()}
		elif mention_match_type == "ew":
			basic_filters_dictionary["user_mentions_lower"] = {'$regex' : mention.lower()+"$"}
		elif mention_match_type == "co":
			basic_filters_dictionary["user_mentions_lower"] = {'$regex' : mention.lower()}
		elif mention_match_type == "em":
			basic_filters_dictionary["user_mentions_lower"] = mention.lower()

	if language != None:
		basic_filters_dictionary["lang"] = language

	if text != None:
		text_match_type = text[:2].lower()
		text = text[3:].lower()
		if text_match_type == "sw":
			basic_filters_dictionary["text_lower"] = {'$regex' : "^"+text.lower()}
		elif text_match_type == "ew":
			basic_filters_dictionary["text_lower"] = {'$regex' : text.lower()+"$"}
		elif text_match_type == "co":
			basic_filters_dictionary["text_lower"] = {'$regex' : text.lower()}
		elif text_match_type == "em":
			basic_filters_dictionary["text_lower"] = text.lower()

	if retweet_count != None:
		rtcountType = retweet_count[:2].lower()
		retweet_count = retweet_count[2:]

		if retweet_count.isdigit():
			if rtcountType == "gt":
				basic_filters_dictionary["retweet_count"] = {'$gt':int(retweet_count)}
			elif rtcountType == "lt":
				basic_filters_dictionary["retweet_count"] = {'$lt':int(retweet_count)}
			elif rtcountType == "eq":
				basic_filters_dictionary["retweet_count"] = {'$eq':int(retweet_count)}
			elif rtcountType == "le":
				basic_filters_dictionary["retweet_count"] = {'$lte':int(retweet_count)}
			elif rtcountType == "ge":
				basic_filters_dictionary["retweet_count"] = {'$gte':int(retweet_count)}

	if favourite_count != None:
		favcountType = favourite_count[:2].lower()
		favourite_count = favourite_count[2:]
		if favourite_count.isdigit():
			if favcountType == "gt":
				basic_filters_dictionary["favorite_count"] = {'$gt':int(favourite_count)}
			elif favcountType == "lt":
				basic_filters_dictionary["favorite_count"] = {'$lt':int(favourite_count)}
			elif favcountType == "eq":
				basic_filters_dictionary["favorite_count"] = {'$eq':int(favourite_count)}
			elif favcountType == "le":
				basic_filters_dictionary["favorite_count"] = {'$lte':int(favourite_count)}
			elif favcountType == "ge":
				basic_filters_dictionary["favorite_count"] = {'$gte':int(favourite_count)}

	users_map = {
					i["id"]: i for i in list(users.find(None,{
																'name':1,
																'screen_name':1,
																'followers_count':1, 
																'location':1, 
																'id':1, 
																'_id':0
															}
														)
											)
				}

	tweets = list(tweets.find(basic_filters_dictionary, {'hashtags_lower':0, 'text_lower':0, 'keyword':0, 
		'user_mentions_lower':0, '_id':0}).sort([(primary_sort, primary_sort_type)]))

	result = [dict(tweet, user=users_map[tweet["user"]]) for tweet in tweets]

	if secondary_sort != None:
		if secondary_sort == "user":
			result = sorted(result, key=lambda k: k['user']['name'])
		elif secondary_sort == "screen_name":
			result = sorted(result, key=lambda k: k['user']['screen_name'])
		elif secondary_sort == "followers_count":
			result = sorted(result, key=lambda k: k['user']['followers_count'])
		if secondary_sort_type == "dsc":
			result = result[::-1]

	return result