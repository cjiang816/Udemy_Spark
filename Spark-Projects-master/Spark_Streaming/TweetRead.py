import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json


# Set up your credentials
consumer_key='6C7aNuGj5zrB8vUIBVSl3KEeW'
consumer_secret='z20zTOtnzqwTEHkoOGKJE38jVZrBveA6UeMd1rpD6AH9R2FLFT'
access_token ='162376305-rtaUx1aE1C4ohtNElu1BV8Cw0kwiFFU0M7maL3O3'
access_secret='A53TXNu1dO65qbzERQyThXHe3CazebkZ10l8RH8lgnCEK'


class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          print( msg['text'].encode('utf-8') )
          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['kpop'])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "127.0.0.1"     # Get local machine name
  port = 9999                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print( "Received request from: " + str( addr ) )

  sendData( c )