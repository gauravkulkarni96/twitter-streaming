from . import app
import json
from datetime import datetime
from bson import ObjectId
from tweepy.streaming import StreamListener
#MongoDB
from flask_pymongo import PyMongo
import pymongo

mongo = PyMongo(app)


#Custom JSON converter to support Datetime and Object types
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
        	return str(o)
        return json.JSONEncoder.default(self, o)

#Function to store curated data in database
def storeData(data, keyword):
	users = mongo.db.users
	tweets = mongo.db.tweets
	dtest = tweets.find_one({'id':data['id']})
	if dtest == None:
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
		savedata = {key: data[key] for key in data_keys}

		if data['truncated'] and 'extended_tweet' in data and 'full_text' in data['extended_tweet']:
			savedata['text'] = data['extended_tweet']['full_text']
		else:
			savedata['text'] = data['text']

		savedata['created_at'] = datetime.strptime(data['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
		savedata['text_lower'] = savedata['text'].lower()
		savedata['hashtags'] = [x['text'] for x in data['entities']['hashtags']]
		savedata['hashtags_lower'] = [x['text'].lower() for x in data['entities']['hashtags']]
		savedata['user_mentions'] = [x['screen_name'] for x in data['entities']['user_mentions']]
		savedata['user_mentions_lower'] = [x['screen_name'].lower() for x in data['entities']['user_mentions']]
		savedata['keyword'] = keyword.lower()

		savedata['is_retweet'] = False
		if 'retweeted_status' in data:
			savedata['is_retweet'] = True

		tweets.insert(savedata)

# StreamListener class for working on streaming tweets
class StdOutListener(StreamListener):
	def __init__(self, time, count, keyword):
		self.maxtweet = count
		self.time = time
		self.tweetcount = 0
		self.starttime = datetime.now()
		self.keyword = keyword

	def on_data(self, data):
		if self.time and (datetime.now()-self.starttime).seconds >= self.time:
			return False

		dtemp = json.loads(data)
		storeData(dtemp, self.keyword)

		self.tweetcount+=1
		if self.time and (datetime.now()-self.starttime).seconds >= self.time:
			return False

		if self.maxtweet and self.tweetcount >= self.maxtweet:
			return False

		return True

	def on_error(self, status):
		print status

#Function to filter data based on applied filters
def filterData(name, text, rtcount, favcount, datestart, dateend, 
				language, mention, sortPar,hashtag, ufollowcount, 
				typeTw, location, keyword):
	users = mongo.db.users
	tweets = mongo.db.tweets

	sort, sortType = "created_at", pymongo.DESCENDING
	sort2=None
	sortType2 = None
	if sortPar != None and '-' in sortPar:
		sorttemp, sortTypetemp = sortPar.strip().split("-")
		if sorttemp and sortTypetemp:
			if sorttemp == "name":
				sort2 = "user"
			elif sorttemp == "sname":
				sort2 = "screen_name"
			elif sorttemp == "followers":
				sort2 = "followers_count"

			elif sorttemp == "text":
				sort = "text"
			elif sorttemp == "fav":
				sort = "favorite_count"
			elif sorttemp == "ret":
				sort = "retweet_count"
			else:
				sort = "created_at"

			if sortTypetemp == "asc":
				if sort2:
					sortType2="asc"
				else:
					sortType = pymongo.ASCENDING
			elif sortTypetemp == "dsc":
				if sort2:
					sortType2="dsc"
				else:
					sortType = pymongo.DESCENDING
		else:
			sort, sortType = "created_at", pymongo.DESCENDING

	filterDictuser = {}
	filterDict = {}

	if keyword != None:
		filterDict["keyword"] = keyword.lower()

	if typeTw != None:
		if typeTw == "retweet":
			filterDict["is_retweet"] = True
		elif typeTw == "quote":
			filterDict["is_quote_status"] = True
		elif typeTw == "original":
			filterDict["is_retweet"] = False
			filterDict["is_quote_status"] = False

	if hashtag != None:
		filterDict['hashtags_lower'] = hashtag.lower()

	if datestart != None and dateend == None and len(datestart)==10:
		try:
			date, month, year = map(int, datestart.strip().split("-"))
			datest = datetime(year,month, date)
			print datest
			filterDict['created_at'] = {'$gte':datest}
		except:
			pass

	if dateend != None and datestart == None and len(dateend)==10:
		try:
			date, month, year = map(int, dateend.strip().split("-"))
			daten = datetime(year,month, date)
			filterDict['created_at'] = {'$lte':daten}
		except:
			pass

	if datestart != None and dateend != None and len(datestart)==10 and len(dateend)==10:
		try:
			date, month, year = map(int, datestart.strip().split("-"))
			datest = datetime(year,month, date)

			date, month, year = map(int, dateend.strip().split("-"))
			daten = datetime(year,month, date)
			filterDict['$and'] = [
									{'created_at':{'$gte':datest}},
								 	{'created_at':{'$lte':daten}}
								 ]
		except:
			pass

	if location != None:
		filterDictuser["location_lower"] = location.lower()

	if ufollowcount != None:
		ufollowcountType = ufollowcount[:2].lower()
		ufollowcount = ufollowcount[2:]
		if ufollowcount.isdigit():
			if ufollowcountType == "gt":
				filterDictuser["followers_count"] = {'$gt':int(ufollowcount)}
			elif ufollowcountType == "lt":
				filterDictuser["followers_count"] = {'$lt':int(ufollowcount)}
			elif ufollowcountType == "eq":
				filterDictuser["followers_count"] = {'$eq':int(ufollowcount)}
			elif ufollowcountType == "le":
				filterDictuser["followers_count"] = {'$lte':int(ufollowcount)}
			elif ufollowcountType == "ge":
				filterDictuser["followers_count"] = {'$gte':int(ufollowcount)}
	
	if name != None:
		nameType = name[:2].lower()
		name = name[3:].lower()

		if nameType == "sw":
			filterDictuser["$or"] = [
						{"name_lower" : {'$regex' : "^"+name.lower()}}, 
						{"screen_name_lower" : {'$regex' : "^"+name.lower()}}
					]

		elif nameType == "ew":
			filterDictuser["$or"] = [
				{"name_lower" : {'$regex' : name.lower()+"$"}}, 
				{"screen_name_lower" : {'$regex' : name.lower()+"$"}}
			]

		elif nameType == "co":
			filterDictuser["$or"] = [
				{"name_lower" : {'$regex' : name.lower()}}, 
				{"screen_name_lower" : {'$regex' : name.lower()}}
			]

		elif nameType == "em":
			filterDictuser["$or"] = [
				{"name_lower" : name.lower()}, 
				{"screen_name_lower" : name.lower()}
			]
		
	if name != None or ufollowcount != None or location != None:
		nameids = []
		for i in users.find(filterDictuser):
			nameids.append(i['id'])
		filterDict['user'] = {'$in' : nameids}

	if mention != None:
		mentionType = mention[:2].lower()
		mention = mention[3:].lower()
		if mentionType == "sw":
			filterDict["user_mentions_lower"] = {'$regex' : "^"+mention.lower()}
		elif mentionType == "ew":
			filterDict["user_mentions_lower"] = {'$regex' : mention.lower()+"$"}
		elif mentionType == "co":
			filterDict["user_mentions_lower"] = {'$regex' : mention.lower()}
		elif mentionType == "em":
			filterDict["user_mentions_lower"] = mention.lower()

	if language != None:
		filterDict["lang"] = language

	if text != None:
		textType = text[:2].lower()
		text = text[3:].lower()
		if textType == "sw":
			filterDict["text_lower"] = {'$regex' : "^"+text.lower()}
		elif textType == "ew":
			filterDict["text_lower"] = {'$regex' : text.lower()+"$"}
		elif textType == "co":
			filterDict["text_lower"] = {'$regex' : text.lower()}
		elif textType == "em":
			filterDict["text_lower"] = text.lower()

	if rtcount != None:
		rtcountType = rtcount[:2].lower()
		rtcount = rtcount[2:]
		print rtcount, rtcountType
		if rtcount.isdigit():
			if rtcountType == "gt":
				filterDict["retweet_count"] = {'$gt':int(rtcount)}
			elif rtcountType == "lt":
				filterDict["retweet_count"] = {'$lt':int(rtcount)}
			elif rtcountType == "eq":
				filterDict["retweet_count"] = {'$eq':int(rtcount)}
			elif rtcountType == "le":
				filterDict["retweet_count"] = {'$lte':int(rtcount)}
			elif rtcountType == "ge":
				filterDict["retweet_count"] = {'$gte':int(rtcount)}

	if favcount != None:
		favcountType = favcount[:2].lower()
		favcount = favcount[2:]
		if favcount.isdigit():
			if favcountType == "gt":
				filterDict["favorite_count"] = {'$gt':int(favcount)}
			elif favcountType == "lt":
				filterDict["favorite_count"] = {'$lt':int(favcount)}
			elif favcountType == "eq":
				filterDict["favorite_count"] = {'$eq':int(favcount)}
			elif favcountType == "le":
				filterDict["favorite_count"] = {'$lte':int(favcount)}
			elif favcountType == "ge":
				filterDict["favorite_count"] = {'$gte':int(favcount)}

	result = []
	for i in tweets.find(filterDict, {'hashtags_lower':0, 'text_lower':0, 'keyword':0, 
		'user_mentions_lower':0}).sort([(sort, sortType)]):

		i['user'] = users.find_one(
				{'id':i['user']},
				{'name':1,'screen_name':1,'followers_count':1, 'location':1, 'id':1}
			)

		result.append(i)

	if sort2 != None:
		if sort2 == "user":
			result = sorted(result, key=lambda k: k['user']['name'])
		elif sort2 == "screen_name":
			result = sorted(result, key=lambda k: k['user']['screen_name'])
		elif sort2 == "followers_count":
			result = sorted(result, key=lambda k: k['user']['followers_count'])
		if sortType2 == "dsc":
			result = result[::-1]

	return result