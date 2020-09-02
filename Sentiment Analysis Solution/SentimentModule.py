# This is the module that will be called upon to perform sentiment analysis of tweets

import nltk
import re
import pickle
from nltk.classify import ClassifierI
from statistics import mode
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer

# This class inherits from NLTK's ClassifierI class - important to define it as a classifier class
# Basically what it does is that it combines all five previously defined classifiers together into a voting system.
# So essentially, a tweet will be categorized by five times, once by each classifier, and the most common resulting polarity from all
# those classifiers will be the chosen polarity. This approach gives us a higher degree of accuracy, reliabilty, and it also
# allows us to provide a confidence score (e.g. if 4/5 classifiers say a tweet is positive, we are 80% confident in the result)
class VotingClassifier(ClassifierI):

    # Constructor will receive a list of five classifiers
    def __init__(self,*classifiers):
        #Storing these five classifiers passed in in a list
        self.classifiers = classifiers

    # This function receives a list of features and returns the most commonly resulting polarity
    def classify(self, features):
        votes = [] # This will store the votes from each classifier
        # Iterating through each classifier
        for classifier in self.classifiers:
            # Using the particular classifier to classify a tweet.
            # The resulting vote will be pos or neg, which
            # will then be stored in the votes list
            vote = classifier.classify(features)
            votes.append(vote)
            # Here I return the most commonly occurring vote
        return mode(votes)

    # This function receives a list of features and returns the confidence for the chosen vote (i.e. most common vote)
    def confidence(self, features):
        votes = []# This will store the votes from each classifier
        # Iterating through each classifier
        for classifier in self.classifiers:
            # Using the particular classifier to classify a tweet.
            # The resulting vote will be pos or neg, which
            # will then be stored in the votes list
            vote = classifier.classify(features)
            votes.append(vote)
        # Here I check how many times the chosen vote occurs
        noOfVotesForChosenPolarity = votes.count(mode(votes))
        # Here I calculate the confidence level in the chosen vote
        conf = noOfVotesForChosenPolarity / len(votes)
        return conf


# We need this because we need to preprocess tweets that are passed in in exactly the same way as the training and testing set.
default_stopwords = set(nltk.corpus.stopwords.words('english'))
custom_stopwords = {"nay", "JetBlue", "JVMChat", "'d", "'s", "n't", "'m", "'ll", "would"}
all_stopwords = default_stopwords.union(custom_stopwords)
# We want to preserve negative stopwords because they have significance
words_to_exclude = {"no","not","couldn't","won't","wasn't","hasn't","shan't","doesn't","aren't","needn't","shouldn't","haven't","don't","isn't","weren't","didn't","mightn't", "mustn't", "wouldn't","hadn't"}
all_stopwords_without_negatives = all_stopwords.difference(words_to_exclude)

lemmatizer = WordNetLemmatizer()
tknzr = TweetTokenizer()# Using this because wordTokenizer seperates for e.g. "wasn't" into "was" and "n't"


# Here I am loading the wordFeatures list from the pickle file it was saved to.
# The wordFeatures list contains the top most common 3000 words from the partOfPreProcessedTweets.csv file
wordFeaturesFile = open("pickleFiles/wordFeatures.pickle", "rb")
wordFeatures = pickle.load(wordFeaturesFile)
wordFeaturesFile.close()

# What this function does is that it takes in a list of words of a particular tweet
# and then it checks which of the top 3000 words are present in this tweet. What
# it will return is a dictionary of all 3000 words as keys, and the values will be
# either true or false, depending on whether each particular word in the top 3000
# words occurs in the list of words of the tweet passed in to this function
def findFeatures(tweet):
    # All we care about here is the unique words in the tweet passed in
    uniqueWordsInTweet = set(tweet)
    features = {}
    # for each word in the top 3000 words
    for word in wordFeatures:
        features[word] = (word in uniqueWordsInTweet)  # This last part is what checks whether a word in the top 3000 words is in the tweet passed in.
    return features


# Here I am loading the NLTK naive bayes classifier from the pickle file it was saved to
naiveBayesClassifierFile = open("pickleFiles/nltkNaiveBayesClassifier.pickle", "rb")
naiveBayesClassifier = pickle.load(naiveBayesClassifierFile)
naiveBayesClassifierFile.close()


# Here I am loading the BernoulliNB classifier from the pickle file it was saved to
BernoulliNBClassifierFile = open("pickleFiles/BernoulliNBClassifier.pickle", "rb")
BernoulliNBClassifier = pickle.load(BernoulliNBClassifierFile)
BernoulliNBClassifierFile.close()


# Here I am loading the LogisticRegression classifier from the pickle file it was saved to
LogisticRegressionClassifierFile = open("pickleFiles/LogisticRegressionClassifier.pickle", "rb")
LogisticRegressionClassifier = pickle.load(LogisticRegressionClassifierFile)
LogisticRegressionClassifierFile.close()


# Here I am loading the SGDC classifier from the pickle file it was saved to
SGDClassifierFile = open("pickleFiles/SGDClassifier.pickle", "rb")
SGDClassifier = pickle.load(SGDClassifierFile)
SGDClassifierFile.close()


# Here I am loading the LinearSVC classifier from the pickle file it was saved to
LinearSVCClassifierFile = open("pickleFiles/LinearSVCClassifier.pickle", "rb")
LinearSVCClassifier = pickle.load(LinearSVCClassifierFile)
LinearSVCClassifierFile.close()


# Here I am creating an instance of the VotingClassifier,
# passing to its constructor all of the other five classifiers
# Note that this classifier does not have to be trained,
# because its constituent classifiers are already trained
VoteClassifier = VotingClassifier(naiveBayesClassifier, BernoulliNBClassifier, LogisticRegressionClassifier, SGDClassifier, LinearSVCClassifier)


# This is the function that will actually perform the sentiment analysis of a passed in tweet or text
def sentiment(text):
    # The first thing we need to do is to preprocess the tweet in EXACTLY the same way the training and testing set were preprocessed
    tokens = []
    # Tokenizing the text
    tokens = tknzr.tokenize(text)
    # Lowercasing all words in the tokenized text
    tokens = [word.lower() for word in tokens]
        # Removing stopwords
    tokens = [word for word in tokens if word not in all_stopwords_without_negatives]
    tokens2 = []
    # Using regular expressions to remove tokens with specific characters
    for token in tokens:
        if re.search(".*=.*", token) == None and re.search(".*[.'-]\b", token) == None and re.search(".*'s", token) == None and re.search("\b[#@.'-].*", token) == None and re.search(".*[/.:;,_+*#@~âã!?^<>{}0-9\\\].*",token) == None and re.search(".*--.*", token) == None:
            tokens2.append(token)

    # Lemmatizing tokens
    tokens = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in tokens2]
    tokens = [lemmatizer.lemmatize(word, wordnet.VERB) for word in tokens]
    tokens = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in tokens]
    tokens = [lemmatizer.lemmatize(word, wordnet.ADV) for word in tokens]

    # Removing tokens that are just 1 character long
    tokens = [word for word in tokens if len(word) > 1]

    # Now we have to convert this list of tokens into a dictionary of features
    # (i.e. the top 3000 words and whether or not they occur in the tweet/text passed in)
    features = findFeatures(tokens)

    # Here we actually classify the tweet and get a confidence for this classification, which we then return
    return VoteClassifier.classify(features), VoteClassifier.confidence(features)
