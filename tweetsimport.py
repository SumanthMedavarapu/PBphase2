from __future__ import absolute_import, print_function
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json

consumer_key="GDKAd6hxJEWyFW4lQQImeg5td"
consumer_secret="O5J0es7A1PzZmdgo4ZIkp11uf1CoyxhiRP804eAZ2RicxuLseC"
access_token="1229634936212250626-0mKi59Mmv67POO0D7hqnwcbH2CGqOF"
access_token_secret="OOLWs7z8lAdbQcMCG6OuVfYL9TmeAapuIlS3QkehCoG6S"

class StdOutListener(StreamListener):
    def on_data(self, data):
        try:
            with open('data10.json', 'a') as outfile:
                json.dump(data,outfile)
            with open('data20.json','a') as outputj:
                outputj.write(data)
            with open('tweetsdata.txt', 'a') as tweets:
                tweets.write(data)
                tweets.write('\n')
            outfile.close()
            tweets.close()
            outputj.close()
        except BaseException as e:
            print('problem collecting tweet',str(e))
        return True
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    #stream.filter(track=['icc','kohli','insta','crypto','happy','win','bitcoin','analytics','python','smart','science','data','apple','iphone','tesla',])
    stream.filter(track=['nike','amazon','walmart','apple','google','yahoo','facebook','at&t','verizon','toyota','samsung','hsbc','microsoft','intel','sony','tesla','nissan','fedex','uber','oracle','cisco','netflix',])




