import praw
from kafka import KafkaProducer
import json
import csv
import subprocess
import os
import mysql.connector

client_id ='5nV8Wwj6VxKrQ9o-lo5qkg'
client_secret ='tNIL7JG0IWh2pbC60bFkOPe5evVZAQ'
username ='test_and_doom'
password ='test@123'
user_agent ='Doomsday'
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="new_password",
  database="reddit"
)


reddit = praw.Reddit(client_id=client_id, client_secret=client_secret,
                     username=username, password=password,
                     user_agent=user_agent)

producer = KafkaProducer(bootstrap_servers='localhost:9092', # Replace with your Kafka bootstrap servers
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

subreddit_name = 'hamilton'
c=0
subreddit = reddit.subreddit(subreddit_name)
with open('output.csv','w',newline='') as f:
	w=csv.writer(f)
	kk=['title','body','author','created_utc','subreddit','score']
	j=['score']
	w.writerow(j)
for submission in subreddit.stream.submissions():
	m=mydb.cursor()
	sql="INSERT INTO proj values(%s);"
	dd=[int(submission.score)]
	m.execute(sql,dd)
	mydb.commit()
	c=c+1    
    # Extract relevant data from Reddit submission object
	try:
		data={
        	'title': submission.title,
        	'body': submission.selftext,
        	'author': submission.author.name,
        	'created_utc': submission.created_utc,
        	'subreddit': submission.subreddit.display_name,
		'score': submission.score}
	except Exception as E:
		pass
	m=[submission.score]
	#k=[submission.title,submission.selftext,submission.author.name,submission.created_utc,submission.subreddit.display_name,submission.score]
	if c>10:
		subprocess.run(['python3','sp.py'])
		with open('sol.csv','r',newline='') as f:
			uu=csv.reader(f)
			ma=0
			for i in uu:
				ma=ma%3
				if ma==0:
					producer.send('redditstream',i)
					producer.flush()
				if ma==1:
					producer.send('avg',i)
					producer.flush()
				if ma==2:
					producer.send('max',i)
					producer.flush()
				ma=ma+1
				print(i)
		os.remove('sol.csv')
		os.remove('output.csv')
		c=0
		with open('output.csv','w',newline='') as f:
			w=csv.writer(f)
			#kk=['title','body','author','created_utc','subreddit','score']
			j=['score']
			w.writerow(j)
	else:	
		with open('output.csv', mode='a', newline='') as f:
    			writer = csv.writer(f)
    			writer.writerow(m)
	print('Published to Kafka:', data)		#writer.writerow(data)
	data={}
	k=[]


    # Print the published data for reference


