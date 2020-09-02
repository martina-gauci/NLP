# This is the file that generates a live updating graph for the polarity of tweets that include an entered keyword
# It is very important that before you run this file, you run TwitterLiveSentimentAnalysis.py first and leave it running

import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style

# This makes the graph a bit better looking
style.use("ggplot")

xValue = 0
yValue = 0

# creating a graph
fig = plt.figure()
ax1 = fig.add_subplot(1, 1, 1)


def animate(i):
    dataFromTwittetOutputFile = open("twitterStreamOutputFile/twitterConfidentTweets.txt", "r").read()
    # Creating a list from the output file by splitting it by newline
    polarities = dataFromTwittetOutputFile.split("\n")

    xArray = []
    yArray = []

    global xValue
    global yValue

    # x and y will start from the xValue and yValue global variables for continuity
    x = xValue
    y = yValue

    for polarity in polarities[-200:]:
        # The x value always increments, regardless of the polarity
        x += 1
        # The y value will increment or decrement based on the current polarity being considered
        if "pos" in polarity:
            y += 1
        else:
            y -= 1

        # After analysing a particular polarity, the new updated x and y values are appended to the arrays
        xArray.append(x)
        yArray.append(y)

    # Resetting the current xValue and yValue global variables
    xValue = xArray[len(xArray) - 1]
    yValue = yArray[len(yArray) - 1]
    # This is important, otherwise many graphs will be drawn on top of each other
    ax1.clear()
    # plotting the graph and setting the title and labels
    ax1.plot(xArray, yArray)
    ax1.set_xlabel('Tweets Considered so far')
    ax1.set_ylabel('Polarity Tendency')
    ax1.set_title('Live Sentiment Score')


# This animation should update every second, as specified by the interval parameter
ani = animation.FuncAnimation(fig, animate, interval=1000)
plt.show()
