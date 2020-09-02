# NOTE - This is the file that should be used for testing the accuracy of each of the classifiers
# The testing set used here DOES NOT overlap with the training set - so we can get an unbiased value for the accuracy

import nltk
import random
import csv
import pickle
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
testingTweets = []

# Populating the testingTweets list
with open('csvDatasets/trainingSet.csv', 'r', encoding="UTF-8") as csvFile:
    readCSV = csv.reader(csvFile)
    for row in readCSV:
        testingTweets.append((row[1].split(), row[0]))

# Shuffling the testingTweets list -  because in the csv file the tweets are highly ordered
for i in range(0,20):
    random.shuffle(testingTweets)

#Taking the first 10,000 tweets only
testingTweets = testingTweets[:10000]

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


# Before, we had the testingTweets list which was a list of tuples, where each tuple corresponds to a
# tweet to be tested, and the first element of that tuple was a list of words in that tweet, and the second
# element was its polarity. Here I am converting this list into another list of tuples, but this
# time the first element of the tuple will be a dictionary of the top 3000 words and a boolean
# value indicating whether or not these words are in the particular tweet to be tested. The
# second element of the tuple will still be the polarity of the tweet.
trainingTweetsFeatureSets = [(findFeatures(tweetToTest), polarity) for (tweetToTest, polarity) in testingTweets]


# Here I am loading the NLTK naive bayes classifier from the pickle file it was saved to
naiveBayesClassifierFile = open("pickleFiles/nltkNaiveBayesClassifier.pickle", "rb")
naiveBayesClassifier = pickle.load(naiveBayesClassifierFile)
naiveBayesClassifierFile.close()
# Here I am checking the accuracy of the classifier by feeding it the testing set
# It will compare its predictions with the actual polarities and comes up with an accuracy percentage
print("NLTK's Naive Bayes Algorithm Accuracy: ", (nltk.classify.accuracy(naiveBayesClassifier, trainingTweetsFeatureSets)) * 100)


# Here I am loading the BernoulliNB classifier from the pickle file it was saved to
BernoulliNBClassifierFile = open("pickleFiles/BernoulliNBClassifier.pickle", "rb")
BernoulliNBClassifier = pickle.load(BernoulliNBClassifierFile)
BernoulliNBClassifierFile.close()
# Testing this classifier to check its accuracy
print("SciKitLearn BernoulliNB Classifier Algorithm Accuracy: ", (nltk.classify.accuracy(BernoulliNBClassifier, trainingTweetsFeatureSets)) * 100)


# Here I am loading the LogisticRegression classifier from the pickle file it was saved to
LogisticRegressionClassifierFile = open("pickleFiles/LogisticRegressionClassifier.pickle", "rb")
LogisticRegressionClassifier = pickle.load(LogisticRegressionClassifierFile)
LogisticRegressionClassifierFile.close()
# Testing this classifier to check its accuracy
print("SciKitLearn LogisticRegression Classifier Algorithm Accuracy: ", (nltk.classify.accuracy(LogisticRegressionClassifier, trainingTweetsFeatureSets)) * 100)


# Here I am loading the SGDC classifier from the pickle file it was saved to
SGDClassifierFile = open("pickleFiles/SGDClassifier.pickle", "rb")
SGDClassifier = pickle.load(SGDClassifierFile)
SGDClassifierFile.close()
# Testing this classifier to check its accuracy
print("SciKitLearn SGDClassifier Classifier Algorithm Accuracy: ", (nltk.classify.accuracy(SGDClassifier, trainingTweetsFeatureSets)) * 100)


# Here I am loading the LinearSVC classifier from the pickle file it was saved to
LinearSVCClassifierFile = open("pickleFiles/LinearSVCClassifier.pickle", "rb")
LinearSVCClassifier = pickle.load(LinearSVCClassifierFile)
LinearSVCClassifierFile.close()
# Testing this classifier to check its accuracy
print("SciKitLearn LinearSVC Classifier Algorithm Accuracy: ", (nltk.classify.accuracy(LinearSVCClassifier, trainingTweetsFeatureSets)) * 100)


# Here I am creating an instance of the VotingClassifier,
# passing to its constructor all of the other five classifiers
# Note that this classifier does not have to be trained,
# because its constituent classifiers are already trained
VoteClassifier = VotingClassifier(naiveBayesClassifier, BernoulliNBClassifier, LogisticRegressionClassifier, SGDClassifier, LinearSVCClassifier)
# Here we are testing the accuracy of the new classifier - it will be the average of all the other classifier accuracies
print("Voting Classifier Algorithm Accuracy: ", (nltk.classify.accuracy(VoteClassifier, trainingTweetsFeatureSets)) * 100)

print("")  # blank space

# This is for us to be able to manually test the accuracy of the VoteClassifier
for x in range(0, 100):
    print("Classsification: ", VoteClassifier.classify(trainingTweetsFeatureSets[x][0]), " Actual polarity: ", trainingTweetsFeatureSets[x][1],
          " Confidence Percentage: ", VoteClassifier.confidence(trainingTweetsFeatureSets[x][0]) * 100)
