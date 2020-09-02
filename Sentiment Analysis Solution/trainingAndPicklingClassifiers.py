# NOTE -- to test the accuracy of the classifiers DO NOT use this file. Before the classifiers were pickled(i.e. stored to a file) it was ok to test here because the training and testing
# datasets were always mutually exclusive (i.e. they did not overlap) as you can see in the code below. If you try to run this now, first of all you will overwrite the pickle files that
# already exist, but it is also very likely that the some of the data that will be tested will have been trained against - which is not good as this creates EXTREME bias.

import nltk
import random
import csv
import pickle
from nltk.classify.scikitlearn import SklearnClassifier  # <-- This is a wrapper to include the scikit learn classification algorithms
from sklearn.naive_bayes import BernoulliNB
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.svm import LinearSVC
from nltk.classify import ClassifierI
from statistics import mode


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



# This will store a list of tuples, where each tuple is a list of words in a preprocessed tweet, and its polarity
tweets = []

# Populating the tweets list
with open('csvDatasets/partOfPreProcessedTweets.csv', 'r', encoding="UTF-8") as csvFile:
    readCSV = csv.reader(csvFile)
    for row in readCSV:
        tweets.append((row[1].split(), row[0]))

# Shuffling the tweets list - very important as in the csv file they are highly ordered
for i in range(0,20):
    random.shuffle(tweets)


# Now I want to create a list of all words in the preprocessed tweets
allWords = []

# Populating the allWords list
with open('csvDatasets/partOfPreProcessedTweets.csv', 'r', encoding="UTF-8") as csvFile:
    readCSV = csv.reader(csvFile)
    for row in readCSV:
        listOfWordsInTweet = []
        # .split converts a string of words into a list of words
        listOfWordsInTweet = row[1].split()
        for word in listOfWordsInTweet:
            allWords.append(word)

# Converting the allWords list into a frequency distribution. In this frequency
# distribution, the most common word will be placed at the front of the list, etc...
allWords = nltk.FreqDist(allWords)

# Now I will get the top most common 3000 words from all tweets and store them in a list
wordFeatures = list(allWords.keys())[:3000]

# Here I am using pickle to save the wordFeatures list to a file
wordFeaturesFile = open("pickleFiles/wordFeatures.pickle", "wb")
pickle.dump(wordFeatures, wordFeaturesFile)
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


# Before, we had the tweets list which was a list of tuples, where each tuple corresponds to a
# tweet, and the first element of that tuple was a list of words in that tweet, and the second
# element was its polarity. Here I am converting this list into another list of tuples, but this
# time the first element of the tuple will be a dictionary of the top 3000 words and a boolean
# value indicating whether or not these words are in the particular tweet. The second element of
# the tuple will still be the polarity of the tweet.
featureSets = [(findFeatures(tweet), polarity) for (tweet, polarity) in tweets]

# We can now split these featureSets into a training and testing set
# I split them using in about an 80/20 ratio - abiding by the pareto principle
trainingSet = featureSets[:51200]
testingSet = featureSets[51200:]


# Here I am creating an nltk naive bayes classifier and training it against the training set
naiveBayesClassifier = nltk.NaiveBayesClassifier.train(trainingSet)
# Here I am checking the accuracy of the classifier by feeding it the testing set
# It will compare its predictions with the actual polarities and comes up with an accuracy percentage
print("NLTK's Naive Bayes Algorithm Accuracy: ", (nltk.classify.accuracy(naiveBayesClassifier, testingSet)) * 100)

# Here I am using pickle to save the classifier - so that I don't have to train it each time
naiveBayesSavedClassifierFile = open("pickleFiles/nltkNaiveBayesClassifier.pickle", "wb")
pickle.dump(naiveBayesClassifier, naiveBayesSavedClassifierFile)
naiveBayesSavedClassifierFile.close()


# Creating a BernoulliNB classifier
BernoulliNBClassifier = SklearnClassifier(BernoulliNB())
# Training the classifier against the training set
BernoulliNBClassifier.train(trainingSet)
# Testing this classifier to check its accuracy
print("SciKitLearn BernoulliNB Classifier Algorithm Accuracy: ", (nltk.classify.accuracy(BernoulliNBClassifier, testingSet)) * 100)

# Here I am using pickle to save the BernoulliNB classifier - so that I don't have to train it each time
BernoulliNBClassifierFile = open("pickleFiles/BernoulliNBClassifier.pickle", "wb")
pickle.dump(BernoulliNBClassifier, BernoulliNBClassifierFile)
BernoulliNBClassifierFile.close()



# Creating a LogisticRegression classifier
LogisticRegressionClassifier = SklearnClassifier(LogisticRegression())
# Training the classifier against the training set
LogisticRegressionClassifier.train(trainingSet)
# Testing this classifier to check its accuracy
print("SciKitLearn LogisticRegression Classifier Algorithm Accuracy: ", (nltk.classify.accuracy(LogisticRegressionClassifier, testingSet)) * 100)

# Here I am using pickle to save the LogisticRegression classifier - so that I don't have to train it each time
LogisticRegressionClassifierFile = open("pickleFiles/LogisticRegressionClassifier.pickle", "wb")
pickle.dump(LogisticRegressionClassifier, LogisticRegressionClassifierFile)
LogisticRegressionClassifierFile.close()



# Creating an SGDClassifier classifier
SGDClassifier = SklearnClassifier(SGDClassifier())
# Training the classifier against the training set
SGDClassifier.train(trainingSet)
# Testing this classifier to check its accuracy
print("SciKitLearn SGDClassifier Classifier Algorithm Accuracy: ", (nltk.classify.accuracy(SGDClassifier, testingSet)) * 100)

# Here I am using pickle to save the SGDC classifier - so that I don't have to train it each time
SGDClassifierFile = open("pickleFiles/SGDClassifier.pickle", "wb")
pickle.dump(SGDClassifier, SGDClassifierFile)
SGDClassifierFile.close()



# Creating a LinearSVC classifier
LinearSVCClassifier = SklearnClassifier(LinearSVC())
# Training the classifier against the training set
LinearSVCClassifier.train(trainingSet)
# Testing this classifier to check its accuracy
print("SciKitLearn LinearSVC Classifier Algorithm Accuracy: ", (nltk.classify.accuracy(LinearSVCClassifier, testingSet)) * 100)

# Here I am using pickle to save the LinearSVC classifier - so that I don't have to train it each time
LinearSVCClassifierFile = open("pickleFiles/LinearSVCClassifier.pickle", "wb")
pickle.dump(LinearSVCClassifier, LinearSVCClassifierFile)
LinearSVCClassifierFile.close()



# Here I am creating an instance of the VotingClassifier,
# passing to its constructor all of the other five classifiers
# Note that this classifier does not have to be trained,
# because its constituent classifiers are already trained
VoteClassifier = VotingClassifier(naiveBayesClassifier, BernoulliNBClassifier, LogisticRegressionClassifier, SGDClassifier, LinearSVCClassifier)
# Here we are testing the  accuracy of the new classifier - it will be the average of all the other classifier accuracies
print("Voting Classifier Algorithm Accuracy: ", (nltk.classify.accuracy(VoteClassifier, testingSet)) * 100)

print("") # blank space

# This is for us to be able to manually test the accuracy of the VoteClassifier
for x in range (0, 100):
    print("Classsification: ", VoteClassifier.classify(testingSet[x][0]), " Actual polarity: ", testingSet[x][1], " Confidence Percentage: ", VoteClassifier.confidence(testingSet[x][0]) * 100)
