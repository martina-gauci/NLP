import csv
import re
import nltk
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer

default_stopwords = set(nltk.corpus.stopwords.words('english'))
custom_stopwords = {"nay", "JetBlue", "JVMChat", "'d", "'s", "n't", "'m", "'ll", "would"}
all_stopwords = default_stopwords.union(custom_stopwords)
#We want to preserve negative stopwords because they have significance
words_to_exclude = {"no","not","couldn't","won't","wasn't","hasn't","shan't","doesn't","aren't","needn't","shouldn't","haven't","don't","isn't","weren't","didn't","mightn't", "mustn't", "wouldn't","hadn't"}
all_stopwords_without_negatives = all_stopwords.difference(words_to_exclude)

lemmatizer = WordNetLemmatizer()
tknzr = TweetTokenizer()#Using this because wordTokenizer seperates for e.g. "wasn't" into "was" and "n't"

categories = []
reviews = []

# Reading all the data into lists
with open('twitterTweetsPosNegRandomized.csv', 'r' ,encoding="UTF-8") as csvFile:
    readCSV = csv.reader(csvFile)
    for row in readCSV:
        categories.append(row[0])
        reviews.append(row[1])

preProcessedReviews = [] #This will store the reviews after they have been preprocessed

for review in reviews:
    tokens = []
    tokens = tknzr.tokenize(review)
    tokens = [word.lower() for word in tokens]
    tokens = [word for word in tokens if word not in all_stopwords_without_negatives]
    tokens2 = []
    for token in tokens:
        if re.search(".*=.*", token) == None and re.search(".*[.'-]\b", token) == None and re.search(".*'s", token) == None and re.search("\b[#@.'-].*", token) == None and re.search(".*[/.:;,_+*#@~âã!?^<>{}0-9\\\].*", token) == None and re.search(".*--.*",token) == None:
            tokens2.append(token)

    # Lemmatizing words
    tokens = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in tokens2]
    tokens = [lemmatizer.lemmatize(word, wordnet.VERB) for word in tokens]
    tokens = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in tokens]
    tokens = [lemmatizer.lemmatize(word, wordnet.ADV) for word in tokens]

    #Removing words that are just 1 character long
    tokens = [word for word in tokens if len(word) > 1]

    print(tokens)

    string_Of_Tokens = ''

    #Here I transform the list of tokens into a string
    if len(tokens) > 0 :
        for x in range(0,len(tokens)-1):
            string_Of_Tokens += tokens[x] + ' '
        string_Of_Tokens += tokens[len(tokens)-1]
    else:
        string_Of_Tokens = ''

    #Appending the preprocessed review to the relevant list
    preProcessedReviews.append(string_Of_Tokens)



print(len(categories), len(preProcessedReviews))

with open('preProcessedTweets.csv', 'w' ,encoding="UTF-8", newline='') as newCSVFile:
    writeCSV = csv.writer(newCSVFile, quoting=csv.QUOTE_ALL)
    for x in range (0,799998):
        writeCSV.writerow([categories[x],preProcessedReviews[x]])
