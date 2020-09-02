# This file shows a prompt to the user, where he/she enters a keyword and then tweets including that keyword are extracted
# from the twitter streaming API, after which they are classified. The polarities of these tweets are written to a file.

# Here I am importing the module we created that actually has the function to classify tweets/text
import SentimentModule
import json
import os
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from ttkthemes import themed_tk as tk
from tkinter import *
from tkinter import ttk

# This class is for streaming tweets live
class listener(StreamListener):
    def on_data(self, data):
        all_data = json.loads(data)
        # Here we get the tweet
        tweet = all_data["text"]
        # Here we pass the tweet through the sentiment function of the imported SentimentModule,
        # and get a classification and a confidence score for that classification
        polarity, confidence = SentimentModule.sentiment(tweet)
        # Printing the tweet, classification (i.e. polarity) and confidence in that classification
        print(tweet, polarity, confidence)

        # Only if all five classifier agree on the polarity of a tweet, then write the polarity of that tweet to a file
        if confidence * 100 == 100:
            outputFile = open("twitterStreamOutputFile/twitterConfidentTweets.txt", "a")
            outputFile.write(polarity)
            outputFile.write('\n')
            outputFile.close()
        return True

    def on_error(self, status):
        print(status)


# consumer key, consumer secret, access token, access secret.
ckey = "tQgDDCNQossoL35sNow9xzauW"
csecret = "6luuE6L2XoaluDVXrjwr5NqP4KyL7f6SZNPEKfvn3ccGN6cIav"
atoken = "1099270357159211009-ODitbyjhLPg1HqZ1BwZJwsXrrjRURY"
asecret = "zdn1FVXBf0zjt4TqqSiK6gjPSCNj44atnoFIxOPSJT7dL"


# We want to delete the output file if it already exists
if os.path.exists("twitterStreamOutputFile/twitterConfidentTweets.txt"):
    os.remove("twitterStreamOutputFile/twitterConfidentTweets.txt")
    print('File Removed')
else:
    print('File does not exist')


auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)


# What this function does is that it gets the keyword/s entered by the user from the tkinter window, and uses them to get tweets from twitter with those keywords
def streamAndClassifyData():
    userEnteredKeywords = userInput.get()
    # If the user has not entered a keyword, show a validation error message on the window
    if len(userEnteredKeywords) == 0:
        validationErrorMessage = ttk.Label(canvas, text="The field cannot be empty!", font=("courier", 16), foreground="red", background=tkinterWindow.cget('bg'))
        validationErrorMessage.grid(row=2, column=0)
    else:
        # Close the window
        tkinterWindow.destroy()
        #Start streaming data
        twitterStream = Stream(auth, listener())
        # Here I get all tweets for the keywords the user entered, which are written in English
        # This is important as our classifiers were trained on english tweets!
        twitterStream.filter(languages=["en"], track=[userEnteredKeywords])



# This basically builds a very simple user interface window
tkinterWindow = tk.ThemedTk()
tkinterWindow.get_themes()
tkinterWindow.set_theme("arc")
tkinterWindow.title("Keyword Input")

style = ttk.Style()
style.configure("Bold.TButton", font = ('monospace','22','bold'))

tkinterWindow.config(height=300, width=1100)
canvas = Canvas(tkinterWindow, highlightthickness=0)
canvas.place(relx=0.5, rely=0.5, anchor=CENTER)

prompt = ttk.Label(canvas, text="Please enter the keywords to analyze sentiment of, separated by a space",background=tkinterWindow.cget('bg'), font = ('courier', 18))
userInput = ttk.Entry(canvas, width=50, font = ('courier', 18))
# On clicking the button, call the streamAndClassifyData() function
submitButton = ttk.Button(canvas, text="Submit", width=20, style="Bold.TButton", command=streamAndClassifyData)

# Here I use the grid system to align the components
prompt.grid(row=0, column=0, padx=20, pady=10)
userInput.grid(row=1, column=0, padx=20, pady=10, ipadx =5, ipady=5)
submitButton.grid(row=3, column=0, padx=20, pady=20)

tkinterWindow.mainloop() #This shows the window
