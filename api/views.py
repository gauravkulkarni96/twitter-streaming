#Flask
from flask import Flask, request, jsonify, make_response

#Tweepy
from tweepy import OAuthHandler
from tweepy import Stream

#Others
import csv

#Utilities
from utils import *
from . import app

#API 1 - To stream data based on keyword
@app.route("/stream/<keyword>", methods=['GET','POST'])
def stream(keyword):
	try:
		time = request.args.get('time')
		count = request.args.get('count')

		if time == None or time == "":
			time = 0
		if count == None or count == "":
			count = 0
		if time == 0 and count ==0:
			return jsonify({"code":"1","status":"failed","message":"No Parameters Passed"})

		l = StdOutListener(int(time), int(count), keyword)
		auth = OAuthHandler(app.config['CONSUMER_KEY'], app.config['CONSUMER_SECRET'])
		auth.set_access_token(app.config['ACCESS_TOKEN'], app.config['ACCESS_TOKEN_SECRET'])
		stream = Stream(auth, l)

		stream.filter(track=[keyword])

		response = {
					"code":"0","status":"success",
					"message":"Successful"
					}
		return jsonify(response)

	except:
		response = {
					"code":"1","status":"failed",
					"message":"Some error occured"
					}
		return jsonify(response)


#API 2 - To filter/search stored tweets 
@app.route("/search", methods=['GET','POST'])
def search():
	try:
		name = request.args.get('name')
		text = request.args.get('text')
		rtcount = request.args.get('rtcount')
		favcount = request.args.get('favcount')
		datestart = request.args.get('datestart')
		dateend = request.args.get('dateend')
		language = request.args.get('lang')	
		mention = request.args.get('mention')
		sortPar = request.args.get('sort')
		page = request.args.get('page')
		hashtag = request.args.get('hashtag')
		ufollowcount = request.args.get('followers')
		typeTw = request.args.get('type')
		location = request.args.get('location')
		keyword = request.args.get('keyword')

		result = filterData(name, text, rtcount, favcount, 
			datestart, dateend, language, mention, sortPar, 
			hashtag, ufollowcount, typeTw, location, keyword)

		limit = 10
		if page == None or not page.isdigit() or int(page)<1:
			page = 1
		else:
			page = int(page)
		next_page = page+1
		last_page = False
		if len(result) <=(page*limit):
			last_page = True
			next_page = 1

		count = len(result)
		resulttemp = result[((page-1)*limit) : (page*limit)]

		if len(resulttemp) == 0:
			resulttemp = result[(0*limit) : (1*limit)]
			page = 1

		return JSONEncoder().encode({'result': resulttemp, 'result_count': count, 
									'page': page, 'next_page':next_page, 
									'last_page':last_page})
	except:
		response = {
					"code":"1","status":"failed",
					"message":"Some error occured"
					}
		return jsonify(response)

#API 3 - To download data in CSV format
@app.route("/getcsv", methods=['GET','POST'])
def getcsv():
	try:
		name = request.args.get('name')
		text = request.args.get('text')
		rtcount = request.args.get('rtcount')
		favcount = request.args.get('favcount')
		datestart = request.args.get('datestart')
		dateend = request.args.get('dateend')
		language = request.args.get('lang')	
		mention = request.args.get('mention')
		sortPar = request.args.get('sort')
		hashtag = request.args.get('hashtag')
		ufollowcount = request.args.get('followers')
		typeTw = request.args.get('type')
		location = request.args.get('location')
		keyword = request.args.get('keyword')

		result = filterData(name, text, rtcount, favcount, 
				 datestart, dateend, language, 
				 mention, sortPar, hashtag, ufollowcount, 
				 typeTw, location, keyword)

		csvfile = "id,created_at,language,user_name,user_screen_name,user_followers,user_location,"\
		"user_id,text,hashtags,mentions,retweet_count,favorite_count,is_retweet,is_quote\n"
		for i in result:
			i['text'] = i['text'].replace("\n", " ")
			csvfile += ",".join('"{0}"'.format(s) for s in [str(i['id']), str(i['created_at']),i['lang'], i['user']['name'].encode('utf-8'), 
				i['user']['screen_name'].encode('utf-8'),str(i['user']['followers_count']),str(i['user']['location']) , 
				str(i['user']['id']), i['text'].encode('utf-8'), "-".join(i['hashtags']), "-".join(i['user_mentions']), 
				str(i['retweet_count']), str(i['favorite_count']),  
				str(i['is_retweet']), str(i['is_quote_status'])]) + "\n"
		response = make_response(csvfile)
		cd = 'attachment; filename=twitterStream.csv'
		response.headers['Content-Disposition'] = cd 
		response.mimetype='text/csv'

		return response
	except:
		response = {
					"code":"1","status":"failed",
					"message":"Some error occured"
					}
		return jsonify(response)