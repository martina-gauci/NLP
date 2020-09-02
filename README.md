# NLP
This repository holds work I have done in NLP and Machine Learning.  
This project was carried out with another 3 students.  My contibution to this project was focues at the backend processing of the Enron dataset, frontend styling for the text forensics solution, display of visualisations, the script used for sentiment analysis and the integration of the Twitter Streaming API in Python.
The technologies I came across in this project include JavaScript, jQuery, Python, Plotly, PostgreSQL and Resful APIs among many others.

## Project Description
In this project we developed two separate tools: a text forensics tool for analysing an email corpus and a Twitter live sentiment analysis tool.

For the text forensics solution, we built a tool that allows an investigator to interrogate an email corpus in many different ways, so that they will be able to find and extract useful information in an efficient and comprehensible manner. This tool allows investigators to explore and investigate relationships between different aspects of the data, such as senders of emails, receivers, dates and content of emails. This tool is be based on the Enron email corpus which we obtained from https://www.cs.cmu.edu/~enron/.  A Python script was written to extract and transform the data into a more structured and ‘queryable’ form and placing it in a database.  A RESTFUL API was built using Flask. At the frontend, a web-based application was built so that the user can extract information about a wide range of aspects, such as correspondence between a set of people within a particular time period, volume of emails sent with particular keywords within a time period and calendar heatmaps. The user can also filter emails and view their full text.  When the investigator asks for specific information, an API request is sent to a particular endpoint and then the API will then return the data in JSON format which is needed to construct the required visualisation. Plotly.js was used to produce the  visualizations. 

For the sentiment analysis solution, we built a Python based tool that graphs live Twitter sentiment for a given topic. First, we sourced a dataset containing labelled tweets, which was preprocessed to transform the words into their root form and remove features like stopwords. Second, a voting classifier was built comprised of a set of other classifiers which ‘vote’ for the polarity of a given tweet. Pre-implemented classifiers from the SciKit Learn library were used.
This approach improves accuracy and reliability of the classifications while also allow us to calculate the confidence in the classification. Using the confidence score, we could disregard any tweets which may be incorrectly classified.  These classifiers were trained and tested against the sourced and preprocessed tweet dataset. Then, the Twitter Streaming API was used to get a live stream of tweets for a particular topic.  Each tweet goes through the same preprocessing steps as the tweets in the training set, after which it is classified. A line graph is displayed to the user where the x-axis represents the number of tweets classified so far, while the y-axis represents the sentiment to the particular topic being analyzed. The y value is incremented by 1 when a streamed tweet is deemed positive or decremented by 1 if the tweet is classified as negative.  Therefore, the y-value represents the sentiment towards the topic in question.

## Project Setup

You will need to install the following Python packages to run the code in this repository: NLTK, MatPlotLib, Plotly, Numpy, SkLearn, Tweepy, SciPy, PsycoPg2, TTKThemes, Flask and Flask-Restful.

To set up the Text Forensics tool follow the steps below:
1. Create and import database from the backup provided

2. Open the API.py file provided, and at the top change the database connection string to contain your own database password.

3. Run the API before the front end application is used.

4. Install the "Allow CORS: Access-Control-Allow-Origin" Chrome extension and turn it on

5. Open the website by clicking on the index.html in the folder

To set up the sentiment analysis tool run TwitterLiveSentimentAnalysis.py.  Once the data streaming starts, run LiveUpdatingGraph.py
