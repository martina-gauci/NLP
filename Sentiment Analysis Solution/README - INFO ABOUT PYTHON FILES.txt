datasetPreProcessing.py is the file that preprocessed the dataset we used for this work.

trainingAndPicklingClassifiers.py generated some pickle files, and thus only needed to be run once. Do not run this, there is no need to, but take a look at the code.

testingClassifierAccuries.py is a file you can run to test the accuracy of all five classifiers used in this work.

SentimentModule.py simply implements a function that takes in a tweet or text, and classifies it as positive or negative and  returns the confidence in that classification.
It is a module , so it is used in another file. On its own it won't do anything.

TwitterLiveSentimentAnalysis.py takes in a keyword or set of keywords, extracts tweets including those keywords using twitter's streaming API and classifies them.
These polarities are then written to a file.

LiveUpdatingGraph.py is simply the file which will generate the live updating sentiment graph
VERY IMPORTANT: You must run TwitterLiveSentimentAnalysis.py before running LiveUpdatingGraph.py!!!!!