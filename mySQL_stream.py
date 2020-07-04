import mysql.connector
from mysql.connector import Error
import tweepy
import json
from textblob import TextBlob

#Method to connect to MySQL database and insert data
def connect(username, created, text3, sent_polar, sent_subj, location, quoted):
    #Connect to MySQL database
    try:
        con = mysql.connector.connect(host='localhost',database='twitterdb', user='root', password='[redacted]', charset='utf8')
        #If connected, INSERT data into specified table
        if con.is_connected():
            #Cursor is used,standard MySQL syntax
            cursor = con.cursor()
            #We need a query as a string which we will pass to the cursor, standard MySQL syntax
            query = "INSERT INTO twitter_table (username, created, text3, sent_polar, sent_subj, location, quoted) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(query, (username, created, text3, sent_polar, sent_subj, location, quoted))
            con.commit()

    #Will print any error we may have
    except Error as e:
        print(e)
    #Closes cursor and connection
    cursor.close()
    con.close()

    return

#Tweepy is used to access Twitter API
#StreamListener refers to a non-stop flow of tweet objects
class Streamlistener(tweepy.StreamListener):

    #Notified when connected successfully
    def on_connect(self):
        print("You are connected to the Twitter API")

    #Allows for reading of json data
    def on_data(self, data):
        try:
            #Loads the tweet object data
            raw_data = json.loads(data)
            if 'text' in raw_data: #Pulls specified values from tweet object
                username = raw_data['user']['screen_name']
                created = raw_data['created_at']
                text = raw_data['text']
                quoted_text = ''
                quoted = 'False'
                if raw_data['is_quote_status'] == True:
                    quoted_obj = raw_data['quoted_status']
                    quoted_text = quoted_obj['text']
                    quoted = 'True'
                #I threw in a TextBlob to showcase how another library can be implemented
                text2 = TextBlob(text + " // " + quoted_text) #not the most efficient
                text3 = text + " // " + quoted_text #not the most efficient
                sent_polar = text2.sentiment.polarity #returns polarity (-neg,0,+pos between -1,1)
                sent_subj = text2.sentiment.subjectivity #same as above but with subjectivity. A confidence score.
                location = raw_data['user']['location']

                #Insert into MySQL database
                connect(username, created, text3, sent_polar, sent_subj, location, quoted)
                print("Tweet collected at: {} ".format(str(created)))
        #Will print any error we may come across
        except Error as e:
            print("ERROR:" + e)

if __name__ == '__main__':
    #Set access tokens
    ACCESS_TOKEN = '[redacted]'
    ACCESS_SECRET = '[redacted]'
    CONSUMER_KEY = '[redacted]'
    CONSUMER_SECRET = '[redacted]'
    #Connect to Twitter API
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    #Create instance of StreamListener
    listener = Streamlistener(api=api)
    stream = tweepy.Stream(auth, listener=listener)
    #Any phrases or terms we want to track
    track = ['nfl']
    #Specify filters to stream data by
    stream.filter(track=track, languages=['en'])