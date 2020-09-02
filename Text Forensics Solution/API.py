import psycopg2
from datetime import datetime
import random
import json
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
import ast
from flask import Flask, request
from flask_restful import Api, Resource

app = Flask(__name__)
api = Api(app)
lemmatizer = WordNetLemmatizer()  # instantiating lemmatizer

try:

    connection = psycopg2.connect(
        user="postgres", password="1234", host="127.0.0.1", port="5432", database="EnronDB")
    cursor = connection.cursor()  # creating a cursor object using the connection object

    # FUNCTIONS COMBINING WORDS AND DATES
    # Most common words within specified time period
    # Returns data for a HeatMap
    class Most_Popular_Words_In_Particular_Year_Grouped_By_Month(Resource):
        def get(self):
            args = request.args
            year = int(args['year'])

            words = []  # on x-axis
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']  # on y-axis
            monthsNumbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
            frequencies = []  # on z-axis

            months = [month + ' ' + str(year) for month in months]

            postgres_select_query1 = '''
                SELECT et.term FROM enron.emails e
                INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                WHERE extract(year from e.date) = %s AND length(et.term) > 3 AND et.term != 'http'
                GROUP BY et.term
                HAVING COUNT(et.term) > 20
                ORDER BY SUM(et.frequency) DESC
                LIMIT 80
            '''

            # Executing the query to get the most popular words in the specified year
            cursor.execute(postgres_select_query1, (year,))
            rows1 = cursor.fetchall()
            for row in rows1:
                words.append(row[0])
            if len(words) == 0:
                return [{'ErrorMessage': 'No emails were sent in this year!'}]
            else:
                words.sort()
                wordsTuple = tuple(words)

                postgres_select_query2 = '''
                    DROP TABLE IF EXISTS termstemp;
                    DROP TABLE IF EXISTS termsfrequency;
                    CREATE TEMPORARY TABLE termstemp(term text);
                    INSERT INTO termstemp
                    SELECT t.term
                    FROM enron.terms t WHERE t.term IN %s
                    ORDER BY term;

                    CREATE TEMPORARY TABLE termsfrequency(term text,freq int);
                    INSERT INTO termsfrequency
                    SELECT et.term, SUM(et.frequency) FROM enron.emails e
                    INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                    WHERE extract(year from e.date) = %s AND extract(month from e.date) = %s AND et.term IN  %s
                    GROUP BY et.term;

                    SELECT COALESCE(tf.freq, 0) FROM termstemp tt LEFT JOIN termsfrequency tf ON tt.term = tf.term;
                '''

                for monthNumber in monthsNumbers:
                    frequencies_per_month_list = []
                    cursor.execute(postgres_select_query2,
                                   (wordsTuple, year, monthNumber, wordsTuple,))
                    rows = cursor.fetchall();
                    for row in rows:
                        frequencies_per_month_list.append(row[0])
                    frequencies.append(frequencies_per_month_list)

                return [{'YearInput': year, 'WordsXAxis': words, 'MonthsYAxis': months,
                        'FrequenciesZAxis': frequencies, 'ErrorMessage': 'None'}]
    # Returns data for a HeatMap

    class Most_Popular_Words_In_Particular_Month_Grouped_By_Day(Resource):
        def get(self):
            args = request.args
            year = int(args['year'])
            month = int(args['month'])

            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']

            # These will be passed to the plotly heatmap
            days = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
                    28, 29, 30, 31]  # on y-axis
            words = []  # on x-axis
            frequencies = []  # on z-axis

            postgres_select_query1 = "\
                SELECT et.term FROM enron.emails e\
                INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid\
                WHERE extract(year from e.date) = %s AND extract(month from e.date) = %s AND length(et.term) > 3 AND et.term != 'http'\
                GROUP BY et.term\
                HAVING COUNT(et.term) > 20\
                ORDER BY SUM(et.frequency) DESC\
                LIMIT 80"

            # Executing the query to get the most popular words in the specified month of the specified year
            cursor.execute(postgres_select_query1, (year, month,))
            rows1 = cursor.fetchall()
            for row in rows1:
                words.append(row[0])
            if len(words) == 0:
                return [{'ErrorMessage': 'No emails were sent in this month!'}]
            else:
                words.sort()
                print(words)
                wordsTuple = tuple(words)

                postgres_select_query2 = '''
                    DROP TABLE IF EXISTS termstemp;
                    DROP TABLE IF EXISTS termsfrequency;
                    CREATE TEMPORARY TABLE termstemp(term text);
                    INSERT INTO termstemp
                    SELECT t.term
                    FROM enron.terms t WHERE t.term IN %s
                    ORDER BY term;

                    CREATE TEMPORARY TABLE termsfrequency(term text,freq int);
                    INSERT INTO termsfrequency
                    SELECT et.term, SUM(et.frequency) FROM enron.emails e
                    INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                    WHERE extract(year from e.date) = %s AND extract(month from e.date) = %s AND extract(day from e.date) = %s AND et.term IN %s
                    GROUP BY et.term;

                    SELECT COALESCE(tf.freq, 0) FROM termstemp tt LEFT JOIN termsfrequency tf ON tt.term = tf.term;
                '''

                for date in days:
                    frequencies_per_day_list = []
                    cursor.execute(postgres_select_query2,
                                   (wordsTuple, year, month, date, wordsTuple,))
                    rows = cursor.fetchall();
                    for row in rows:
                        frequencies_per_day_list.append(row[0])
                    frequencies.append(frequencies_per_day_list)

                return [{'YearInput': year, 'MonthInput': months[month - 1], 'WordsXAxis': words,
                        'DaysYAxis': days, 'FrequenciesZAxis': frequencies, 'ErrorMessage': 'None'}]
    # Returns data for a BarChart

    class Most_Popular_Words_In_Particular_Day(Resource):
        def get(self):
            args = request.args
            year = int(args['year'])
            month = int(args['month'])
            day = int(args['day'])

            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']

            if month == 2:
                if year%4 ==0:
                    if day>29:
                        return [{'ErrorMessage': months[month-1] + ' '+ str(day) +' '+ str(year)+' IS NOT A VALID DATE'}]
                elif day>28:
                    return [{'ErrorMessage': months[month-1] + ' '+ str(day) +' '+ str(day)+ ' IS NOT A VALID DATE'}]
            elif month==4 or month==6 or month==9 or month==11:
                if day>30:
                    return [{'ErrorMessage': months[month-1] + ' '+ str(day) +' '+ str(day) + ' IS NOT A VALID DATE'}]
            else:
                if day>31:
                    return [{'ErrorMessage': months[month-1] + ' '+ str(day) +' '+ str(day) + ' IS NOT A VALID DATE'}]

            # These will be passed to the plotly bargraph
            words = []  # on x-axis
            frequencies = []  # on y-axis

            postgres_select_query1 = "\
                SELECT et.term FROM enron.emails e\
                INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid\
                WHERE extract(year from e.date) = %s AND extract(month from e.date) = %s AND extract(day from e.date) = %s AND length(et.term) > 3 AND et.term != 'http'\
                GROUP BY et.term\
                HAVING COUNT(et.term) > 20\
                ORDER BY SUM(et.frequency) DESC\
                LIMIT 80"

            # Executing the query to get the most popular words in the specified day of the specified month of the specified year
            cursor.execute(postgres_select_query1, (year, month, day,))
            rows1 = cursor.fetchall()
            for row in rows1:
                words.append(row[0])
            if len(words) == 0:
                return [{'ErrorMessage': 'No emails were sent on this day!'}]
            else:
                words.sort()  # Sorting the words alphabetically
                print(words)
                print(len(words))
                wordsTuple = tuple(words)

                postgres_select_query2 = '''
                    DROP TABLE IF EXISTS termstemp;
                    DROP TABLE IF EXISTS termsfrequency;
                    CREATE TEMPORARY TABLE termstemp(term text);
                    INSERT INTO termstemp
                    SELECT t.term
                    FROM enron.terms t WHERE t.term IN %s
                    ORDER BY term;

                    CREATE TEMPORARY TABLE termsfrequency(term text,freq int);
                    INSERT INTO termsfrequency
                    SELECT et.term, SUM(et.frequency) FROM enron.emails e
                    INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                    WHERE extract(year from e.date) = %s AND extract(month from e.date) = %s AND extract(day from e.date) = %s AND et.term IN %s
                    GROUP BY et.term;

                    SELECT COALESCE(tf.freq, 0) FROM termstemp tt LEFT JOIN termsfrequency tf ON tt.term = tf.term;
                '''

                cursor.execute(postgres_select_query2, (wordsTuple, year, month, day, wordsTuple,))
                rows = cursor.fetchall()
                for row in rows:
                    frequencies.append(row[0])

                return [{'YearInput': year, 'MonthInput': months[month - 1], 'DayInput': day, 'WordsXAxis': words,
                        'FrequenciesYAxis': frequencies, 'ErrorMessage': 'None'}]

    # When were specified words mentioned the most? (most popular years, months, or days)
    # Returns data for a HeatMap
    class Most_Popular_Years_Words_Were_Mentioned(Resource):
        def get(self):
            args = request.args
            wordsList = ast.literal_eval(args['wordList'])  # This translates the string into the appropriate data type

            if len(wordsList) == 0:
                return [{'ErrorMessage': 'You did not enter any word!'}]
            else:
                # Lemmatizing the words that user input
                wordsList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in wordsList]

                wordsTuple = tuple(wordsList)
                # These will be passed to the plotly heatmap
                words = []  # Will be on the y-axis
                years = []  # Will be on the x-axis
                frequencies = []  # Will be on the axis

                postgres_select_query1 = 'SELECT term FROM enron.terms WHERE term IN %s'

                # Filtering only words which are in the database's terms table
                cursor.execute(postgres_select_query1, (wordsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    words.append(row[0])

                if len(words) == 0:
                    return [{'ErrorMessage': 'The word/s you entered are not in the corpus'}]
                else:
                    words.sort()  # Sorting the words alphabetically

                    # Selecting top 10 years when a word was mentioned (Even though there probably aren't 10 years)
                    postgres_select_query2 = '''
                         SELECT to_char(e.date, 'yyyy'), SUM(et.frequency) FROM enron.emails e
                         INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                         WHERE et.term = %s AND (to_char(e.date, 'yyyy') LIKE '1___' OR to_char(e.date, 'yyyy') LIKE '2___')
                         GROUP BY to_char(e.date, 'yyyy') 
                         ORDER BY SUM(et.frequency) DESC
                         LIMIT 10;
                     '''

                    # Executing the above query on the words provided
                    for word in words:
                        cursor.execute(postgres_select_query2, (word,))
                        rows2 = cursor.fetchall()
                        for row in rows2:
                            years.append(row[0])

                    # Removing duplicate years and ordering them (oldest to newest)
                    yearsSet = set(years)
                    years = list(yearsSet)
                    years.sort(key=lambda date: datetime.strptime(date, '%Y'))
                    yearsTuple = tuple(years)

                    # This query is to see how many times a particular word was mentioned in a particular year
                    postgres_select_query3 = '''
                        DROP TABLE IF EXISTS datestemp;
                        DROP TABLE IF EXISTS datesFrequency;
                        CREATE TEMPORARY TABLE datestemp(dates date);
                        INSERT INTO datestemp(dates)
                        SELECT date_trunc('year',date) FROM enron.emails
                        WHERE to_char(date, 'yyyy') IN %s
                        ORDER by date;

                        --SELECT DISTINCT dates, to_char(dates, 'yyyy') as date1 FROM datestemp ORDER BY dates;

                        CREATE TEMPORARY TABLE datesFrequency(dates date,freq int);
                        INSERT INTO datesFrequency
                        SELECT e.date, SUM(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE et.term = %s
                        GROUP BY e.date
                        ORDER BY e.date;

                        --SELECT to_char(dates, 'yyyy') as date2, SUM(freq) as frequency FROM datesFrequency GROUP BY to_char(dates, 'yyyy');

                        SELECT x.date1,COALESCE(y.frequency,0) FROM (SELECT DISTINCT dates, to_char(dates, 'yyyy') as date1 FROM datestemp ORDER BY dates)x LEFT JOIN 
                        (SELECT to_char(dates, 'yyyy') as date2, SUM(freq) as frequency FROM datesFrequency GROUP BY to_char(dates, 'yyyy'))y
                        ON x.date1 = y.date2
                        ORDER BY x.dates;
                    '''

                    # Executing the above query on the words provided
                    for word in words:
                        wordFrequencies = []
                        cursor.execute(postgres_select_query3, (yearsTuple, word,))
                        rows3 = cursor.fetchall()
                        for row in rows3:
                            wordFrequencies.append(row[1])
                        frequencies.append(wordFrequencies)

                    years = ['Year ' + str(year) for year in years]

                    return [{'YearsXAxis': years, 'WordsYAxis': words, 'FrequenciesZAxis': frequencies,
                            'ErrorMessage': 'None'}]
    # Returns data for a HeatMap
    class Most_Popular_Months_Words_Were_Mentioned(Resource):
        def get(self):
            args = request.args
            wordsList = ast.literal_eval(args['wordList'])  # This translates the string into the appropriate data type

            if len(wordsList) == 0:
                return [{'ErrorMessage': 'You did not enter any word!'}]
            else:
                # Lemmatizing the words that user input
                wordsList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in wordsList]

                wordsTuple = tuple(wordsList)
                # These will be passed to the plotly heatmap
                words = []  # Will be on the y-axis
                dates = []  # Will be on the x-axis
                frequencies = []  # Will be on the axis

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

                # Filtering only words which are in the database terms table
                cursor.execute(postgres_select_query1, (wordsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    words.append(row[0])

                if len(words) == 0:
                    return [{'ErrorMessage': 'The word/s you entered are not in the corpus'}]
                else:
                    words.sort()

                    # Selecting top 10 dates (Month-Year) when a word was mentioned
                    postgres_select_query2 = '''
                        SELECT to_char(e.date, 'Mon yyyy'), et.term, SUM(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE et.term = %s
                        GROUP BY to_char(e.date, 'Mon yyyy'), et.term
                        ORDER BY SUM(et.frequency) DESC
                        LIMIT 10
                    '''

                    # Executing the above query on the words provided
                    for word in words:
                        cursor.execute(postgres_select_query2, (word,))
                        rows2 = cursor.fetchall()
                        for row in rows2:
                            dates.append(row[0])

                    # Removing duplicate dates and ordering them (oldest to newest)
                    datesSet = set(dates)
                    dates = list(datesSet)
                    dates.sort(key=lambda date: datetime.strptime(date, '%b %Y'))
                    datesTuple = tuple(dates)

                    # This query is to see how many times a particular word was mentioned in a particular month of a particular year
                    postgres_select_query3 = '''
                        DROP TABLE IF EXISTS datestemp;
                        DROP TABLE IF EXISTS datesFrequency;
                        CREATE TEMPORARY TABLE datestemp(dates date);
                        INSERT INTO datestemp(dates)
                        SELECT date_trunc('month',date) FROM enron.emails
                        WHERE to_char(date, 'Mon yyyy') IN %s
                        ORDER by date;

                        CREATE TEMPORARY TABLE datesFrequency(dates date,freq int);
                        INSERT INTO datesFrequency
                        SELECT e.date, SUM(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE et.term = %s
                        GROUP BY e.date
                        ORDER BY e.date;

                        SELECT x.date1,COALESCE(y.frequency,0) FROM (SELECT DISTINCT dates, to_char(dates, 'Mon yyyy') as date1 FROM datestemp ORDER BY dates)x LEFT JOIN 
                        (SELECT to_char(dates, 'Mon yyyy') as date2, SUM(freq) as frequency FROM datesFrequency GROUP BY to_char(dates, 'Mon yyyy'))y
                        ON x.date1 = y.date2
                        ORDER BY x.dates;
                    '''

                    # Executing the above query on the words provided
                    for word in words:
                        wordFrequencies = []
                        cursor.execute(postgres_select_query3, (datesTuple, word,))
                        rows3 = cursor.fetchall()
                        for row in rows3:
                            wordFrequencies.append(row[1])
                        frequencies.append(wordFrequencies)

                    return [{'DatesXAxis': dates, 'WordsYAxis': words, 'FrequenciesZAxis': frequencies,
                            'ErrorMessage': 'None'}]
    # Returns data for a HeatMap
    class Most_Popular_Days_Words_Were_Mentioned(Resource):
        def get(self):
            args = request.args
            wordsList = ast.literal_eval(args['wordList'])  # This translates the string into the appropriate data type

            if len(wordsList) == 0:
                return [{'Error Message': 'You did not enter any word!'}]
            else:
                # Lemmatizing the words that user input
                wordsList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in wordsList]

                wordsTuple = tuple(wordsList)
                # These will be passed to the plotly heatmap
                words = []  # Will be on the y-axis
                dates = []  # Will be on the x-axis
                frequencies = []  # Will be on the z-axis

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

                # Filtering only words which are in the database terms table
                cursor.execute(postgres_select_query1, (wordsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    words.append(row[0])

                if len(words) == 0:
                    return [{'Error Message': 'The word/s you entered are not in the corpus'}]
                else:
                    words.sort()  # Sorting the words alphabetically

                    # Selecting top 20 dates (Day-Month-Year) when a word was mentioned
                    postgres_select_query2 = '''
                        SELECT to_char(e.date, 'DD Mon yyyy'), et.term, SUM(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE et.term = %s
                        GROUP BY to_char(e.date, 'DD Mon yyyy'), et.term
                        ORDER BY SUM(et.frequency) DESC
                        LIMIT 20
                    '''

                    # Executing the above query on each of the words provided
                    for word in words:
                        cursor.execute(postgres_select_query2, (word,))
                        rows2 = cursor.fetchall()
                        for row in rows2:
                            dates.append(row[0])

                    # Removing duplicate dates and ordering them (oldest to newest)
                    datesSet = set(dates)
                    dates = list(datesSet)
                    dates.sort(key=lambda date: datetime.strptime(date, '%d %b %Y'))
                    datesTuple = tuple(dates)

                    # This query is to see how many times a particular word was mentioned in a particular day of a particular month of a particular year
                    postgres_select_query3 = '''
                        DROP TABLE IF EXISTS datestemp;
                        DROP TABLE IF EXISTS datesFrequency;
                        CREATE TEMPORARY TABLE datestemp(dates date);
                        INSERT INTO datestemp(dates)
                        SELECT date FROM enron.emails
                        WHERE to_char(date, 'DD Mon yyyy') IN %s
                        ORDER by date;

                        --SELECT DISTINCT dates, to_char(dates, 'DD Mon yyyy') as date1 FROM datestemp ORDER BY dates;

                        CREATE TEMPORARY TABLE datesFrequency(dates date,freq int);
                        INSERT INTO datesFrequency
                        SELECT date_trunc('day',e.date), SUM(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE et.term = %s
                        GROUP BY date_trunc('day',e.date)
                        ORDER BY date_trunc('day',e.date);

                        --SELECT to_char(dates, 'DD Mon yyyy') as date2, freq as frequency FROM datesFrequency;

                        SELECT x.date1,COALESCE(y.frequency,0) FROM (SELECT DISTINCT dates, to_char(dates, 'DD Mon yyyy') as date1 FROM datestemp ORDER BY dates)x 
                        LEFT JOIN (SELECT to_char(dates, 'DD Mon yyyy') as date2, freq as frequency FROM datesFrequency)y
                        ON x.date1 = y.date2
                        ORDER by x.dates;
                    '''

                    # Executing the above query on the words provided
                    for word in words:
                        wordFrequencies = []
                        cursor.execute(postgres_select_query3, (datesTuple, word,))
                        rows3 = cursor.fetchall()
                        for row in rows3:
                            wordFrequencies.append(row[1])
                        frequencies.append(wordFrequencies)

                    return [{'DatesXAxis': dates, 'WordsYAxis': words, 'FrequencyZAxis': frequencies,
                            'ErrorMessage': 'None'}]

    # How many times were specified terms mentioned within specified time period?
    # Returns data for a Heatmap
    class Word_Mentions_In_Year_Grouped_By_Month(Resource):
        def get(self):
            args = request.args
            wordsList = ast.literal_eval(args['wordList'])  # This translates the string into the appropriate data type
            year = int(args['year'])

            if len(wordsList) == 0:
                return [{'ErrorMessage': 'You did not enter any word!'}]
            else:
                # Lemmatizing the words that user input
                wordsList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in wordsList]

                wordsTuple = tuple(wordsList)
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']  # On x-axis
                words = []  # On y-axis
                frequency = []  # On z-axis

                months = [month + ' ' + str(year) for month in months]

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

                # Filtering only words which are in the database's terms table
                cursor.execute(postgres_select_query1, (wordsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    words.append(row[0])
                if len(words) == 0:
                    return [{'ErrorMessage': 'The word/s you entered are not in the corpus'}]
                else:
                    words.sort()  # Sorting the words alphabetically

                    # This query is to see how many times a particular word occurred per month in a particular year
                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS months;
                        DROP TABLE IF EXISTS wordFrequencyPerMonth;
                        CREATE TEMPORARY TABLE months(monthID date);
                        INSERT INTO months(monthID)
                        VALUES('2000-01-01'),('2000-02-01'),('2000-03-01'),('2000-04-01'),('2000-05-01'),('2000-06-01'),('2000-07-01'),('2000-08-01'),('2000-09-01'),('2000-10-01'),('2000-11-01'),('2000-12-01');

                        --SELECT monthID, to_char(monthID, 'Month') as monthInText1 FROM months ORDER by monthID;

                        CREATE TEMPORARY TABLE wordFrequencyPerMonth(monthID text, freq int);
                        INSERT INTO wordFrequencyPerMonth(monthID, freq)
                        SELECT to_char(e.date, 'Month'), SUM(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE et.term = %s AND EXTRACT(YEAR FROM e.date) = %s
                        GROUP BY to_char(e.date, 'Month');

                        --SELECT monthID as monthInText2, freq as frequency FROM wordFrequencyPerMonth;

                        SELECT x.monthInText1, COALESCE(y.frequency,0) FROM (SELECT monthID as month, to_char(monthID, 'Month') as monthInText1 FROM months ORDER by monthID)x
                        LEFT JOIN(SELECT monthID as monthInText2, freq as frequency FROM wordFrequencyPerMonth)y ON x.monthInText1 = y.monthInText2
                        ORDER BY x.month;
                    '''

                    for word in words:
                        wordFrequencyPerMonth = []
                        cursor.execute(postgres_select_query2, (word, year,))
                        rows2 = cursor.fetchall()
                        for row in rows2:
                            wordFrequencyPerMonth.append(row[1])
                        frequency.append(wordFrequencyPerMonth)

                    return [{'YearInput': year, 'MonthsXAxis': months, 'WordsYAxis': words,
                            'FrequenciesZAxis': frequency, 'ErrorMessage': 'None'}]
    # Returns data for a HeatMap
    class Word_Mentions_In_Month_Grouped_By_Day(Resource):
        def get(self):
            args = request.args
            wordsList = ast.literal_eval(args['wordList'])  # This translates the string into the appropriate data type
            year = int(args['year'])
            month = int(args['month'])

            if len(wordsList) == 0:
                return [{'ErrorMessage': 'You did not enter any word!'}]
            else:
                # Lemmatizing the words that user input
                wordsList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in wordsList]

                wordsTuple = tuple(wordsList)
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']
                daysInMonth = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                               25, 26, 27, 28, 29, 30, 31]
                daysInMonth = [str(months[month - 1]) + ' ' + str(day) for day in daysInMonth]  # On x-axis
                words = []  # On y-axis
                frequency = []  # On z-axis

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

                # Filtering only words which are in the database terms table
                cursor.execute(postgres_select_query1, (wordsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    words.append(row[0])
                if len(words) == 0:
                    return [{'ErrorMessage': 'The word/s you entered are not in the corpus'}]
                else:
                    words.sort()  # Sorting the words alphabetically

                    # This query is to see how many times a particular word occurred per day in a particular month of a particular year
                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS daysInMonth;
                        DROP TABLE IF EXISTS wordFrequencyPerDay;
                        CREATE TEMPORARY TABLE daysInMonth(dayID date);
                        INSERT INTO daysInMonth(dayId)
                        VALUES('2000-01-01'),('2000-01-02'),('2000-01-03'),('2000-01-04'),('2000-01-05'),('2000-01-06'),('2000-01-07'),('2000-01-08'),('2000-01-09'),('2000-01-10'),('2000-01-11'),('2000-01-12'),('2000-01-13'),('2000-01-14'),('2000-01-15'),('2000-01-16'),('2000-01-17'),('2000-01-18'),('2000-01-19'),('2000-01-20'),('2000-01-21'),('2000-01-22'),('2000-01-23'),('2000-01-24'),('2000-01-25'),('2000-01-26'),('2000-01-27'),('2000-01-28'),('2000-01-29'),('2000-01-30'),('2000-01-31');

                        --SELECT dayID as day, to_char(dayID, 'DD') as dayInText1 FROM daysInMonth ORDER by dayID;

                        CREATE TEMPORARY TABLE wordFrequencyPerDay(dayID text, freq int);
                        INSERT INTO wordFrequencyPerDay(dayID, freq)
                        SELECT to_char(e.date, 'DD'), SUM(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE et.term = %s AND EXTRACT(YEAR FROM e.date) = %s AND EXTRACT(MONTH FROM e.date) = %s
                        GROUP BY to_char(e.date, 'DD');

                        --SELECT dayID as dayInText2, freq as frequency FROM wordFrequencyPerDay;

                        SELECT x.dayInText1, COALESCE(y.frequency,0) FROM (SELECT dayID as day, to_char(dayID, 'DD') as dayInText1 FROM daysInMonth ORDER by dayID)x
                        LEFT JOIN(SELECT dayID as dayInText2, freq as frequency FROM wordFrequencyPerDay)y ON x.dayInText1 = y.dayInText2
                        ORDER BY x.day;
                    '''

                    for word in words:
                        wordFrequencyPerDay = []
                        cursor.execute(postgres_select_query2, (word, year, month,))
                        rows2 = cursor.fetchall()
                        for row in rows2:
                            wordFrequencyPerDay.append(row[1])
                        frequency.append(wordFrequencyPerDay)

                    return [{'YearInput': year, 'MonthInput': months[month - 1], 'DaysInMonthXAxis': daysInMonth,
                            'WordsYAxis': words, 'FrequenciesZAxis': frequency, 'ErrorMessage': 'None'}]
    # Returns data for a BarChart
    class Word_Mentions_In_Particular_Day(Resource):
        def get(self):
            args = request.args
            wordsList = ast.literal_eval(args['wordList'])  # This translates the string into the appropriate data type
            year = int(args['year'])
            month = int(args['month'])
            day = int(args['day'])

            if len(wordsList) == 0:
                return [{'ErrorMessage': 'You did not enter any word!'}]
            else:
                # Lemmatizing the words that user input
                wordsList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in wordsList]
                wordsList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in wordsList]

                wordsTuple = tuple(wordsList)
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']

                if month == 2:
                    if year%4 ==0:
                        if day>29:
                            return [{'ErrorMessage': months[month-1] + ' '+ str(day) +' '+ str(year)+' IS NOT A VALID DATE'}]
                    elif day>28:
                        return [{'ErrorMessage': months[month-1] + ' '+ str(day) +' '+ str(year)+ ' IS NOT A VALID DATE'}]
                elif month==4 or month==6 or month==9 or month==11:
                    if day>30:
                        return [{'ErrorMessage': months[month-1] + ' '+ str(day) +' '+ str(year) + ' IS NOT A VALID DATE'}]
                else:
                    if day>31:
                        return [{'ErrorMessage': months[month-1] + ' '+ str(day) +' '+ str(year) + ' IS NOT A VALID DATE'}]

                words = []  # On x-axis
                frequency = []  # On y-axis

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

                # Filtering only words which are in the database terms table
                cursor.execute(postgres_select_query1, (wordsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    words.append(row[0])
                if len(words) == 0:
                    return [{'ErrorMessage': 'The word/s you entered are not in the corpus'}]
                else:
                    words.sort()  # Sorting the words alphabetically
                    wordTuple = tuple(words)

                    # This query is to see how many times a particular word occurred on a particular day of a particular month of a particular year
                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS termstemp;
                        DROP TABLE IF EXISTS termsfrequency;
                        CREATE TEMPORARY TABLE termstemp(term text);
                        INSERT INTO termstemp
                        SELECT t.term
                        FROM enron.terms t WHERE t.term IN %s
                        ORDER BY term;

                        CREATE TEMPORARY TABLE termsfrequency(term text,freq int);
                        INSERT INTO termsfrequency
                        SELECT et.term, SUM(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE extract(year from e.date) = %s AND extract(month from e.date) = %s AND extract(day from e.date) = %s AND et.term IN %s
                        GROUP BY et.term;

                        SELECT tt.term, COALESCE(tf.freq, 0) FROM termstemp tt LEFT JOIN termsfrequency tf ON tt.term = tf.term ORDER BY tt.term;
                    '''

                    cursor.execute(postgres_select_query2, (wordTuple, year, month, day, wordTuple))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        frequency.append(row[1])

                    return [{'YearInput': year, 'MonthInput': months[month - 1], 'DayInput': day,
                            'WordsXAxis': words, 'FrequenciesYAxis': frequency, 'ErrorMessage': 'None'}]

    # Comparative Analysis Functions
    # Frequency of three terms over specified time period
    # Returns data for LinePlot with 3 Lines
    class three_term_frequency_comparative_analysis_over_single_year(Resource):
        def get(self):
            args = request.args
            three_words_list = ast.literal_eval(args['threeWordsList']) #This translates the string into the appropriate data type
            year = int(args['year'])

            if len(three_words_list) != 3:
                return [{'ErrorMessage' : 'You have to enter 3 terms!'}]
            else:
                # Lemmatizing the words that user input
                three_words_list = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in three_words_list]
                three_words_list = [lemmatizer.lemmatize(word, wordnet.VERB) for word in three_words_list]
                three_words_list = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in three_words_list]
                three_words_list = [lemmatizer.lemmatize(word, wordnet.ADV) for word in three_words_list]

                termsTuple = tuple(three_words_list)
                termsList = []
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']  # This will be on the x-axis
                # Three lines will be drawn on the line plot
                frequencies_word_1 = []  # On y-axis
                frequencies_word_2 = []  # On y-axis
                frequencies_word_3 = []  # On y-axis

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

                # Filtering only terms which are in the database's terms table
                cursor.execute(postgres_select_query1, (termsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    termsList.append(row[0])
                if len(termsList) != 3:
                    return [{'ErrorMessage': 'A word/s from the three words you entered are not in the corpus!'}]
                else:
                    termsList.sort()  # Sorting the terms alphabetically
                    # Now we have to find the frequency of each term per month in the given year

                    # This query is to see how many times a given word occurred per month in a particular year
                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS months;
                        DROP TABLE IF EXISTS wordFrequencyPerMonth;
                        CREATE TEMPORARY TABLE months(monthID date);
                        INSERT INTO months(monthID)
                        VALUES('2000-01-01'),('2000-02-01'),('2000-03-01'),('2000-04-01'),('2000-05-01'),('2000-06-01'),('2000-07-01'),('2000-08-01'),('2000-09-01'),('2000-10-01'),('2000-11-01'),('2000-12-01');

                        --SELECT monthID, to_char(monthID, 'Month') as monthInText1 FROM months ORDER by monthID;

                        CREATE TEMPORARY TABLE wordFrequencyPerMonth(monthID text, freq int);
                        INSERT INTO wordFrequencyPerMonth(monthID, freq)
                        SELECT to_char(e.date, 'Month'), SUM(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE et.term = %s AND EXTRACT(YEAR FROM e.date) = %s
                        GROUP BY to_char(e.date, 'Month');

                        --SELECT monthID as monthInText2, freq as frequency FROM wordFrequencyPerMonth;

                        SELECT x.monthInText1, COALESCE(y.frequency,0) FROM (SELECT monthID as month, to_char(monthID, 'Month') as monthInText1 FROM months ORDER by monthID)x
                        LEFT JOIN(SELECT monthID as monthInText2, freq as frequency FROM wordFrequencyPerMonth)y ON x.monthInText1 = y.monthInText2
                        ORDER BY x.month;
                    '''

                    cursor.execute(postgres_select_query2, (termsList[0], year,))
                    rows = cursor.fetchall()
                    for row in rows:
                        frequencies_word_1.append(row[1])

                    cursor.execute(postgres_select_query2, (termsList[1], year,))
                    rows = cursor.fetchall()
                    for row in rows:
                        frequencies_word_2.append(row[1])

                    cursor.execute(postgres_select_query2, (termsList[2], year,))
                    rows = cursor.fetchall()
                    for row in rows:
                        frequencies_word_3.append(row[1])

                    return [{'YearInput' : year, 'WordsInput' : termsList, 'MonthsXAxis': months, 'FrequencyOFWord1YAxis' : frequencies_word_1, 'FrequencyOFWord2YAxis' : frequencies_word_2, 'FrequencyOFWord3YAxis' : frequencies_word_3, 'ErrorMessage' : 'None'}]
    # Returns data for LinePlot with 3 Lines
    class three_term_frequency_comparative_analysis_over_single_month(Resource):
        def get(self):
            args = request.args
            three_words_list = ast.literal_eval(args['threeWordsList']) #This translates the string into the appropriate data type
            year = int(args['year'])
            month = int(args['month'])

            if len(three_words_list) != 3:
                return [{'ErrorMessage' : 'You have to enter 3 terms!'}]
            else:
                # Lemmatizing the words that user input
                three_words_list = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in three_words_list]
                three_words_list = [lemmatizer.lemmatize(word, wordnet.VERB) for word in three_words_list]
                three_words_list = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in three_words_list]
                three_words_list = [lemmatizer.lemmatize(word, wordnet.ADV) for word in three_words_list]

                termsTuple = tuple(three_words_list)
                termsList = []
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                daysInMonth = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]  # This will be on the x-axis
                # Three lines will be drawn on the line plot
                frequencies_word_1 = []  # On y-axis
                frequencies_word_2 = []  # On y-axis
                frequencies_word_3 = []  # On y-axis

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

                # Filtering only terms which are in the database's terms table
                cursor.execute(postgres_select_query1, (termsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    termsList.append(row[0])
                if len(termsList) != 3:
                    return [{'ErrorMessage': 'A word/s you entered are not in the corpus!'}]
                else:
                    termsList.sort()  # Sorting the terms alphabetically
                    # Now we have to find the frequency of each term per day in the given month

                    # This query is to see how many times a particular word occurred per day in a particular month of a particular year
                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS daysInMonth;
                        DROP TABLE IF EXISTS wordFrequencyPerDay;
                        CREATE TEMPORARY TABLE daysInMonth(dayID date);
                        INSERT INTO daysInMonth(dayId)
                        VALUES('2000-01-01'),('2000-01-02'),('2000-01-03'),('2000-01-04'),('2000-01-05'),('2000-01-06'),('2000-01-07'),('2000-01-08'),('2000-01-09'),('2000-01-10'),('2000-01-11'),('2000-01-12'),('2000-01-13'),('2000-01-14'),('2000-01-15'),('2000-01-16'),('2000-01-17'),('2000-01-18'),('2000-01-19'),('2000-01-20'),('2000-01-21'),('2000-01-22'),('2000-01-23'),('2000-01-24'),('2000-01-25'),('2000-01-26'),('2000-01-27'),('2000-01-28'),('2000-01-29'),('2000-01-30'),('2000-01-31');

                        --SELECT dayID as day, to_char(dayID, 'DD') as dayInText1 FROM daysInMonth ORDER by dayID;

                        CREATE TEMPORARY TABLE wordFrequencyPerDay(dayID text, freq int);
                        INSERT INTO wordFrequencyPerDay(dayID, freq)
                        SELECT to_char(e.date, 'DD'), SUM(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE et.term = %s AND EXTRACT(YEAR FROM e.date) = %s AND EXTRACT(MONTH FROM e.date) = %s
                        GROUP BY to_char(e.date, 'DD');

                        --SELECT dayID as dayInText2, freq as frequency FROM wordFrequencyPerDay;

                        SELECT x.dayInText1, COALESCE(y.frequency,0) FROM (SELECT dayID as day, to_char(dayID, 'DD') as dayInText1 FROM daysInMonth ORDER by dayID)x
                        LEFT JOIN(SELECT dayID as dayInText2, freq as frequency FROM wordFrequencyPerDay)y ON x.dayInText1 = y.dayInText2
                        ORDER BY x.day;
                    '''

                    cursor.execute(postgres_select_query2, (termsList[0], year, month))
                    rows = cursor.fetchall()
                    for row in rows:
                        frequencies_word_1.append(row[1])

                    cursor.execute(postgres_select_query2, (termsList[1], year, month))
                    rows = cursor.fetchall()
                    for row in rows:
                        frequencies_word_2.append(row[1])

                    cursor.execute(postgres_select_query2, (termsList[2], year, month))
                    rows = cursor.fetchall()
                    for row in rows:
                        frequencies_word_3.append(row[1])

                    return [{'YearInput' : year, 'MonthInput' : months[month - 1], 'WordsInput' : termsList, 'DaysInMonthXAxis': daysInMonth, 'FrequencyOFWord1YAxis' : frequencies_word_1, 'FrequencyOFWord2YAxis' : frequencies_word_2, 'FrequencyOFWord3YAxis' : frequencies_word_3, 'ErrorMessage' : 'None'}]

    # Comparative Analysis Functions
    # Volume of emails sent over three specified years or months
    # Returns data for LinePlot with 3 Lines
    class three_year_volume_of_emails_sent_with_particular_terms_comparative_analysis(Resource):
        def get(self):
            args = request.args
            year1 = int(args['year1'])
            year2 = int(args['year2'])
            year3 = int(args['year3'])
            termsListIn = ast.literal_eval(
                args['wordList'])  # This translates the string into the appropriate data type

            if year1 < 1900 or year1 > 2019 or year2 < 1900 or year2 > 2019 or year3 < 1900 or year3 > 2019:
                return [{'ErrorMessage': 'One or more of the years you entered are invalid!'}]
            elif len(termsListIn) == 0:
                return [{'ErrorMessage': 'You did not enter any terms!'}]
            else:
                # Lemmatizing the words that user input
                termsListIn = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.VERB) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.ADV) for word in termsListIn]

                termsTupleIn = tuple(termsListIn)
                terms = []
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']  # This will be on the x-axis
                # Three lines will be drawn on the line plot
                volume_year1 = []  # On y-axis
                volume_year2 = []  # On y-axis
                volume_year3 = []  # On y-axis

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

                # Filtering only terms which are in the database's terms table
                cursor.execute(postgres_select_query1, (termsTupleIn,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    terms.append(row[0])
                if len(terms) == 0:
                    return [{'ErrorMessage': 'No word you entered is in the corpus!'}]
                else:
                    terms.sort()  # Sorting the terms alphabetically
                    termsTuple = tuple(terms)

                    # Now we have to find the number of emails sent per month in each of the three years (Where the emails contain one or more of the words)

                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS months;
                        DROP TABLE IF EXISTS emailsSentPerMonth;
                        CREATE TEMPORARY TABLE months(monthID date);
                        INSERT INTO months(monthID)
                        VALUES('2000-01-01'),('2000-02-01'),('2000-03-01'),('2000-04-01'),('2000-05-01'),('2000-06-01'),('2000-07-01'),('2000-08-01'),('2000-09-01'),('2000-10-01'),('2000-11-01'),('2000-12-01');

                        --SELECT monthID, to_char(monthID, 'Month') as monthInText1 FROM months ORDER by monthID;

                        CREATE TEMPORARY TABLE emailsSentPerMonth(monthID text, volume int);
                        INSERT INTO emailsSentPerMonth(monthID, volume)
                        SELECT to_char(derivedTable.date, 'Month'), COUNT(derivedTable.date) FROM
                        (
                        SELECT DISTINCT e.emailid as emailid, e.date as date FROM enron.emails e 
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE EXTRACT(YEAR FROM e.date) = %s AND et.term IN %s
                        ORDER BY e.emailid) derivedTable
                        GROUP BY to_char(derivedTable.date, 'Month');

                        --SELECT * FROM emailsSentPerMonth;

                        SELECT to_char(m.monthID, 'Month'), COALESCE(espm.volume,0) FROM months m LEFT JOIN emailsSentPerMonth espm ON to_char(m.monthID, 'Month') = espm.monthID ORDER BY m.monthID;
                    '''

                    cursor.execute(postgres_select_query2, (year1, termsTuple,))
                    rows = cursor.fetchall()
                    for row in rows:
                        volume_year1.append(row[1])

                    cursor.execute(postgres_select_query2, (year2, termsTuple,))
                    rows = cursor.fetchall()
                    for row in rows:
                        volume_year2.append(row[1])

                    cursor.execute(postgres_select_query2, (year3, termsTuple,))
                    rows = cursor.fetchall()
                    for row in rows:
                        volume_year3.append(row[1])

                    return [{'Year1': year1, 'Year2': year2, 'Year3': year3, 'WordsInput': terms,
                            'MonthsXAxis': months, 'VolumeOfEmailsSentInYear1YAxis': volume_year1,
                            'VolumeOfEmailsSentInYear2YAxis)': volume_year2,
                            'VolumeOfEmailsSentInYear3YAxis': volume_year3, 'ErrorMessage': 'None'}]
    # Returns data for LinePlot with 3 Lines
    class three_month_volume_of_emails_sent_with_particular_terms_comparative_analysis(Resource):
        def get(self):
            args = request.args
            year1 = int(args['year1'])
            month1 = int(args['month1'])
            year2 = int(args['year2'])
            month2 = int(args['month2'])
            year3 = int(args['year3'])
            month3 = int(args['month3'])
            termsListIn = ast.literal_eval(
                args['wordList'])  # This translates the string into the appropriate data type

            if year1 < 1900 or year1 > 2019 or year2 < 1900 or year2 > 2019 or year3 < 1900 or year3 > 2019:
                return [{'ErrorMessage': 'One or more of the years you entered are invalid!'}]
            elif month1 < 1 or month1 > 12 or month2 < 1 or month2 > 12 or month3 < 1 or month3 > 12:
                return [{'ErrorMessage': 'One or more of the months you entered are invalid!'}]
            elif len(termsListIn) == 0:
                return [{'ErrorMessage': 'You did not enter any terms!'}]
            else:
                # Lemmatizing the words that user input
                termsListIn = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.VERB) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.ADV) for word in termsListIn]

                termsTupleIn = tuple(termsListIn)
                terms = []
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']

                daysInMonth = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                               25, 26, 27, 28, 29, 30, 31]  # This will be on the x-axis
                # Three lines will be drawn on the line plot
                volume_month1 = []  # On y-axis
                volume_month2 = []  # On y-axis
                volume_month3 = []  # On y-axis

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

                # Filtering only terms which are in the database's terms table
                cursor.execute(postgres_select_query1, (termsTupleIn,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    terms.append(row[0])
                if len(terms) == 0:
                    return [{'ErrorMessage': 'No word you entered is in the corpus!'}]
                else:
                    terms.sort()  # Sorting the terms alphabetically
                    termsTuple = tuple(terms)

                    # Now we have to find the number of emails sent per day in each of the three months (Where the emails contain one or more of the words specified)
                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS daysInMonth;
                        DROP TABLE IF EXISTS emailVolumePerDay;
                        CREATE TEMPORARY TABLE daysInMonth(dayID date);
                        INSERT INTO daysInMonth(dayId)
                        VALUES('2000-01-01'),('2000-01-02'),('2000-01-03'),('2000-01-04'),('2000-01-05'),('2000-01-06'),('2000-01-07'),('2000-01-08'),('2000-01-09'),('2000-01-10'),('2000-01-11'),('2000-01-12'),('2000-01-13'),('2000-01-14'),('2000-01-15'),('2000-01-16'),('2000-01-17'),('2000-01-18'),('2000-01-19'),('2000-01-20'),('2000-01-21'),('2000-01-22'),('2000-01-23'),('2000-01-24'),('2000-01-25'),('2000-01-26'),('2000-01-27'),('2000-01-28'),('2000-01-29'),('2000-01-30'),('2000-01-31');

                        CREATE TEMPORARY TABLE emailVolumePerDay(dayID text, volume int);
                        INSERT INTO emailVolumePerDay(dayID, volume)
                        SELECT to_char(derivedTable.date, 'DD'), COUNT(derivedTable.date) FROM
                        (
                        SELECT DISTINCT e.emailid as emailid, e.date as date FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        WHERE EXTRACT(YEAR FROM e.date) = %s AND EXTRACT(MONTH FROM e.date) = %s
                        AND et.term IN %s
                        ) derivedTable
                        GROUP BY to_char(derivedTable.date, 'DD');

                        SELECT x.dayInText1, COALESCE(y.volume,0) FROM (SELECT dayID as day, to_char(dayID, 'DD') as dayInText1 FROM daysInMonth ORDER by dayID)x
                        LEFT JOIN(SELECT dayID as dayInText2, volume as volume FROM emailVolumePerDay)y ON x.dayInText1 = y.dayInText2
                        ORDER BY x.day;
                    '''

                    cursor.execute(postgres_select_query2, (year1, month1, termsTuple,))
                    rows = cursor.fetchall()
                    for row in rows:
                        volume_month1.append(row[1])

                    cursor.execute(postgres_select_query2, (year2, month2, termsTuple,))
                    rows = cursor.fetchall()
                    for row in rows:
                        volume_month2.append(row[1])

                    cursor.execute(postgres_select_query2, (year3, month3, termsTuple,))
                    rows = cursor.fetchall()
                    for row in rows:
                        volume_month3.append(row[1])

                    return [{'Year1': year1, 'Month1': months[month1 - 1], 'Year2': year2, 'Month2': months[month2 - 1],
                            'Year3': year3, 'Month3': months[month3 - 1], 'WordsInput': terms,
                            'DaysInMonthXAxis': daysInMonth,
                            'VolumeOfEmailsSentInMonth1YAxis': volume_month1,
                            'VolumeOfEmailsSentInMonth2YAxis': volume_month2,
                            'VolumeOfEmailsSentInMonth3YAxis': volume_month3, 'ErrorMessage': 'None'}]



    # FUNCTIONS COMBINING PEOPLE AND TIME
    # Who sent most emails or received most emails in a specified date range?
    # Returns data for a BarChart
    class Most_Frequent_Senders_Within_Specified_Date_Range(Resource):
        def get(self):
            args = request.args
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

            if startMonth==2:
                if startYear%4 ==0:
                    if startDay>29:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startDay>28:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
            elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                if startDay>30:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
            else:
                if startDay>31:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

            if endMonth==2:
                if endYear%4 ==0:
                    if startDay>29:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endDay>28:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
            elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                if endMonth>30:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
            else:
                if endDay>31:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

            senders = []  # On x-axis
            frequency = []  # On y-axis

            # First we have to find the most popular senders within the specified date range
            postgres_select_query1 = '''
                 SELECT p.emailaddress, COUNT(p.emailaddress) FROM enron.emails e
                 INNER JOIN enron.people p ON e.senderid = p.personid
                 WHERE e.date BETWEEN %s AND %s
                 GROUP BY p.emailaddress
                 ORDER BY COUNT(p.emailaddress) DESC
                 LIMIT 50
             '''

            # Getting most popular senders within specified date range
            cursor.execute(postgres_select_query1, (startDate, endDate))
            rows1 = cursor.fetchall()
            for row in rows1:
                senders.append(row[0])
            if len(senders) == 0:
                return [{'ErrorMessage' : 'No one sent an email within the specified date range'}]
            else:
                senders.sort()  # Sorting sender emails alphabetically
                sendersTuple = tuple(senders)
                print(sendersTuple)
                # Now we have to find how many emails these senders sent within the specified time period

                postgres_select_query2 = '''
                     SELECT p.emailaddress, COUNT(p.emailaddress) FROM enron.emails e
                     INNER JOIN enron.people p ON e.senderid = p.personid
                     WHERE e.date BETWEEN %s AND %s
                     AND p.emailaddress IN %s
                     GROUP BY p.emailaddress
                     ORDER BY p.emailaddress
                 '''

                # Getting how many emails each sender sent within the specified date range
                cursor.execute(postgres_select_query2, (startDate, endDate, sendersTuple,))
                rows2 = cursor.fetchall()
                for row in rows2:
                    frequency.append(row[1])

                return [{'StartDate' : str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear), 'EndDate' : str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear), 'SendersXAxis' : senders, "FrequenciesYAxis" : frequency, 'ErrorMessage' : 'None'}]
    # Returns data for a BarChart
    class Most_Frequent_Receivers_Within_Specified_Date_Range(Resource):
        def get(self):
            args = request.args
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

            if startMonth==2:
                if startYear%4 ==0:
                    if startDay>29:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startDay>28:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
            elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                if startDay>30:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
            else:
                if startDay>31:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

            if endMonth==2:
                if endYear%4 ==0:
                    if startDay>29:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endDay>28:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
            elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                if endMonth>30:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
            else:
                if endDay>31:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

            receivers = []  # On x-axis
            frequency = []  # On y-axis

            # First we have to find the most popular receivers within the specified date range
            postgres_select_query1 = '''
                 SELECT r.emailaddress, COUNT(r.emailaddress) FROM enron.emails e
                 INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid)r ON e.emailid = r.emailid
                 WHERE e.date BETWEEN %s AND %s
                 GROUP BY r.emailaddress
                 ORDER BY COUNT(r.emailaddress) DESC
                 LIMIT 50;
             '''

            # Getting most popular receivers within specified date range
            cursor.execute(postgres_select_query1, (startDate, endDate))
            rows1 = cursor.fetchall()
            for row in rows1:
                if "'" not in row[0]:
                    receivers.append(row[0])
            if len(receivers) == 0:
                return [{'ErrorMessage' : 'No one received an email within the specified date range'}]
            else:
                receivers.sort()  # Sorting receiver emails alphabetically
                receiversTuple = tuple(receivers)
                print(receiversTuple)
                # Now we have to find how many emails these receivers received within the specified time period

                postgres_select_query2 = '''
                     SELECT r.emailaddress, COUNT(r.emailaddress) FROM enron.emails e
                     INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid)r ON e.emailid = r.emailid
                     WHERE e.date BETWEEN %s AND %s
                     AND r.emailaddress IN %s
                     GROUP BY r.emailaddress
                     ORDER BY r.emailaddress;
                 '''

                # Getting how many emails each receiver received within the specified date range
                cursor.execute(postgres_select_query2, (startDate, endDate, receiversTuple,))
                rows2 = cursor.fetchall()
                for row in rows2:
                    frequency.append(row[1])

                return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear), 'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear), 'ReceiversXAxis': receivers, "FrequenciesYAxis": frequency, 'ErrorMessage' : 'None'}]

    # Volume of emails sent by specified senders or received by specified receivers within specified date range
    # Returns data for a BarChart
    class How_Many_Emails_Senders_Sent_Within_Time_Period(Resource):
        def get(self):
            args = request.args
            sendersInList = ast.literal_eval(
                args['senderList'])  # This translates the string into the appropriate data type
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            if sendersInList == 0:
                return [{'ErrorMessage': 'You did not enter any sender emails!'}]
            else:
                startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                
                senders = []  # On x-axis
                frequency = []  # On y-axis

                # First we have to filter out only the emails which are in the database's people table (To filter out non-existent emails sent in)
                sendersInTuple = tuple(sendersInList)
                postgres_select_query1 = 'SELECT emailaddress FROM enron.people WHERE emailaddress IN %s ORDER BY emailaddress;'

                cursor.execute(postgres_select_query1, (sendersInTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    senders.append(row[0])

                if len(senders) == 0:
                    return [{'ErrorMessage': 'No sender emails you entered were found in the corpus!'}]
                else:
                    senders.sort()  # Sorting sender emails alphabetically
                    sendersTuple = tuple(senders)

                    # Now we have to find how many emails these senders sent within the specified time period
                    # Query to find how many emails these senders sent within the specified time period
                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS senderEmails;
                        DROP TABLE IF EXISTS sendersFrequencyOFEmails;
                        CREATE TEMPORARY TABLE senderEmails(emailaddress text);
                        INSERT INTO senderEmails
                        SELECT p.emailaddress
                        FROM enron.people p WHERE p.emailaddress IN %s
                        ORDER BY p.emailaddress;

                        CREATE TEMPORARY TABLE sendersFrequencyOFEmails(emailaddress text,freq int);
                        INSERT INTO sendersFrequencyOFEmails
                        SELECT p.emailaddress, COUNT(p.emailaddress) FROM enron.emails e
                        INNER JOIN enron.people p ON e.senderid = p.personid
                        WHERE e.date BETWEEN %s AND %s
                        AND p.emailaddress IN %s
                        GROUP BY p.emailaddress
                        ORDER BY p.emailaddress;

                        SELECT se.emailaddress, COALESCE(sfoe.freq,0) FROM senderEmails se LEFT JOIN sendersFrequencyOFEmails sfoe ON se.emailaddress = sfoe.emailaddress ORDER BY se.emailaddress;
                    '''

                    # Getting how many emails each sender sent within the specified time period
                    cursor.execute(postgres_select_query2, (sendersTuple, startDate, endDate, sendersTuple,))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        frequency.append(row[1])

                    return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear),
                            'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                            'SendersXAxis': senders, 'FrequenciesYAxis': frequency, 'ErrorMessage': 'None'}]
    # Returns data for a BarChart
    class How_Many_Emails_Receivers_Received_Within_Time_Period(Resource):
        def get(self):
            args = request.args
            receiversInList = ast.literal_eval(
                args['receiverList'])  # This translates the string into the appropriate data type
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            if receiversInList == 0:
                return [{'Error Message': 'You did not enter any receiver emails!'}]
            else:
                startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                receivers = []  # On x-axis
                frequency = []  # On y-axis

                # First we have to filter out only the emails which are in the database's people table (To filter out non-existent emails sent in)
                receiversInTuple = tuple(receiversInList)
                postgres_select_query1 = 'SELECT emailaddress FROM enron.people WHERE emailaddress IN %s ORDER BY emailaddress;'

                cursor.execute(postgres_select_query1, (receiversInTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    receivers.append(row[0])

                if len(receivers) == 0:
                    return [{'Error Message': 'No receiver emails you entered were found in the corpus!'}]
                else:
                    receivers.sort()  # Sorting receiver emails alphabetically
                    receiversTuple = tuple(receivers)

                    # Now we have to find how many emails these receivers received within the specified time period
                    # Query to find how many emails these receivers received within the specified time period
                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS receiverEmails;
                        DROP TABLE IF EXISTS receiverFrequencyOFEmails;
                        CREATE TEMPORARY TABLE receiverEmails(emailaddress text);
                        INSERT INTO receiverEmails
                        SELECT p.emailaddress
                        FROM enron.people p WHERE p.emailaddress IN %s
                        ORDER BY p.emailaddress;

                        CREATE TEMPORARY TABLE receiverFrequencyOFEmails(emailaddress text,freq int);
                        INSERT INTO receiverFrequencyOFEmails
                        SELECT r.emailaddress, COUNT(r.emailaddress) FROM enron.emails e
                        INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid)r ON e.emailid = r.emailid
                        WHERE e.date BETWEEN %s AND %s
                        AND r.emailaddress IN %s
                        GROUP BY r.emailaddress
                        ORDER BY r.emailaddress;

                        SELECT re.emailaddress, COALESCE(rfoe.freq,0) FROM receiverEmails re LEFT JOIN receiverFrequencyOFEmails rfoe ON re.emailaddress = rfoe.emailaddress ORDER BY re.emailaddress;
                    '''

                    # Getting how many emails each receiver received within the specified time period
                    cursor.execute(postgres_select_query2, (receiversTuple, startDate, endDate, receiversTuple,))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        frequency.append(row[1])

                    return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear),
                            'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                            'ReceiversXAxis': receivers, 'FrequenciesYAxis': frequency,
                            'ErrorMessage': 'None'}]



    # FUNCTIONS COMBINING PEOPLE, WORDS AND TIME
    # Who sent or received specified words the most within a specified date range?
    # Returns data for a HeatMap
    class Most_Frequent_Senders_Of_Words_Within_Time_Period(Resource):
        def get(self):
            args = request.args
            wordsInList = ast.literal_eval(
                args['wordList'])  # This translates the string into the appropriate data type
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

            if len(wordsInList) == 0:
                return [{'ErrorMessage': 'You did not enter any words!'}]
            else:
                # Lemmatizing the words that user input
                wordsInList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in wordsInList]

                wordsInTuple = tuple(wordsInList)
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                words = []  # On the x-axis
                senders = []  # On the y-axis
                frequencies = []  # On the z-axis

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s ORDER BY term;"

                # Filtering only terms which are in the database's terms table
                cursor.execute(postgres_select_query1, (wordsInTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    words.append(row[0])
                if len(words) == 0:
                    return [{'ErrorMessage': 'The word/s you entered were not found in the corpus'}]
                else:
                    words.sort()  # Sorting words alphabetically
                    # At this point we want to find the most popular senders of these words within a specified time period

                    postgres_select_query2 = '''
                         SELECT p.emailaddress , SUM(et.frequency) FROM enron.emails e
                         INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                         INNER JOIN enron.people p ON e.senderid = p.personid
                         WHERE et.term IN %s
                         AND e.date BETWEEN %s AND %s
                         GROUP BY p.emailaddress
                         ORDER BY SUM(et.frequency) DESC
                         LIMIT 40
                     '''

                    wordsTuple = tuple(words)
                    startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                    endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                    cursor.execute(postgres_select_query2, (wordsInTuple, startDate, endDate,))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        senders.append(row[0])

                    if len(senders) == 0:
                        return [{
                            'ErrorMessage': 'No senders have been found that sent the specified word/s within the specified time period!'}]
                    else:
                        senders.sort()  # Sorting the senders alphabetically
                        # Now we have the words list and the senders list, and we have to find the frequencies

                        # This query finds how many times a particular sender mentioned each of the specified words within the specified date range
                        postgres_select_query3 = '''
                             DROP TABLE IF EXISTS termstemp;
                             DROP TABLE IF EXISTS termsfrequency;
                             CREATE TEMPORARY TABLE termstemp(term text);
                             INSERT INTO termstemp
                             SELECT t.term
                             FROM enron.terms t WHERE t.term IN %s
                             ORDER BY term;

                             CREATE TEMPORARY TABLE termsfrequency(term text,freq int);
                             INSERT INTO termsfrequency
                             SELECT et.term , SUM(et.frequency) FROM enron.emails e
                             INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                             INNER JOIN enron.people p ON e.senderid = p.personid
                             WHERE p.emailaddress = %s AND et.term IN %s
                             AND e.date BETWEEN %s AND %s
                             GROUP BY et.term
                             ORDER BY et.term;

                             SELECT tt.term, COALESCE(tf.freq,0) FROM termstemp tt LEFT JOIN termsfrequency tf ON tt.term = tf.term ORDER BY tt.term
                         '''

                        for sender in senders:
                            words_Frequencies_per_sender = []
                            cursor.execute(postgres_select_query3, (wordsTuple, sender, wordsTuple, startDate, endDate))
                            rows3 = cursor.fetchall()
                            for row in rows3:
                                words_Frequencies_per_sender.append(row[1])
                            frequencies.append(words_Frequencies_per_sender)

                        return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear),
                                'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                                'WordsXAxis': words, 'SendersYAxis': senders,
                                "FrequenciesZAxis": frequencies, 'ErrorMessage': 'None'}]


    # Returns data for a HeatMap
    class Most_Frequent_Receivers_Of_Words_Within_Time_Period(Resource):
        def get(self):
            args = request.args
            wordsInList = ast.literal_eval(
                args['wordList'])  # This translates the string into the appropriate data type
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

            if len(wordsInList) == 0:
                return [{'ErrorMessage': 'You did not enter any words!'}]
            else:
                # Lemmatizing the words that user input
                wordsInList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in wordsInList]

                wordsInTuple = tuple(wordsInList)
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                words = []  # On the x-axis
                receivers = []  # On the y-axis
                frequencies = []  # On the z-axis

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s ORDER BY term;"

                # Filtering only terms which are in the database's terms table
                cursor.execute(postgres_select_query1, (wordsInTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    words.append(row[0])
                if len(words) == 0:
                    return [{'ErrorMessage': 'The word/s you entered were not found in the corpus'}]
                else:
                    words.sort()  # Sorting words alphabetically
                    # At this point we want to find the most popular receivers of these words within a specified time period

                    postgres_select_query2 = '''
                         SELECT r.emailaddress , SUM(et.frequency) FROM enron.emails e
                         INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                         INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid)r ON e.emailid = r.emailid
                         WHERE et.term IN %s
                         AND e.date BETWEEN %s AND %s
                         GROUP BY r.emailaddress
                         ORDER BY SUM(et.frequency) DESC
                         LIMIT 40	
                     '''

                    wordsTuple = tuple(words)
                    startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                    endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                    cursor.execute(postgres_select_query2, (wordsInTuple, startDate, endDate,))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        receivers.append(row[0])

                    if len(receivers) == 0:
                        return [{
                            'ErrorMessage': 'No receivers have been found that received the specified word/s within the specified time period!'}]
                    else:
                        receivers.sort()  # Sorting the receivers alphabetically
                        # Now we have the words list and the receivers list, and we have to find the frequencies

                        # This query finds how many times a particular receiver received each of the specified words within the specified date range
                        postgres_select_query3 = '''
                            DROP TABLE IF EXISTS termstemp;
                            DROP TABLE IF EXISTS termsfrequency;
                            CREATE TEMPORARY TABLE termstemp(term text);
                            INSERT INTO termstemp
                            SELECT t.term
                            FROM enron.terms t WHERE t.term IN %s
                            ORDER BY term;

                            CREATE TEMPORARY TABLE termsfrequency(term text,freq int);
                            INSERT INTO termsfrequency
                            SELECT et.term, SUM(et.frequency) FROM enron.emails e
                            INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                            INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid)r ON e.emailid = r.emailid
                            WHERE r.emailaddress = %s AND et.term IN %s
                            AND e.date BETWEEN %s AND %s
                            GROUP BY et.term
                            ORDER BY et.term;

                            SELECT tt.term, COALESCE(tf.freq,0) FROM termstemp tt LEFT JOIN termsfrequency tf ON tt.term = tf.term ORDER BY tt.term
                        '''

                        for receiver in receivers:
                            words_Frequencies_per_Receiver = []
                            cursor.execute(postgres_select_query3,
                                           (wordsTuple, receiver, wordsTuple, startDate, endDate))
                            rows3 = cursor.fetchall()
                            for row in rows3:
                                words_Frequencies_per_Receiver.append(row[1])
                            frequencies.append(words_Frequencies_per_Receiver)

                        return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear),
                                'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                                'WordsXAxis': words, 'ReceiversYAxis': receivers,
                                "FrequenciesZAxis": frequencies, 'ErrorMessage': 'None'}]

    # How many of a specified set of words did specified senders send or specified receivers receive within a specified date range?
    # Returns data for a HeatMap
    class Senders_Words_Frequency_Within_Time_Period(Resource):
        def get(self):
            args = request.args
            senderEmailList = ast.literal_eval(
                args['senderList'])  # This translates the string into the appropriate data type
            wordsInList = ast.literal_eval(
                args['wordList'])  # This translates the string into the appropriate data type
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

            if len(senderEmailList) == 0:
                return [{'ErrorMessage': 'You did not enter any sender emails!'}]
            elif len(wordsInList) == 0:
                return [{'ErrorMessage': 'You did not enter any words!'}]
            else:
                # Lemmatizing the words that user input
                wordsInList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in wordsInList]

                senderEmailsTuple = tuple(senderEmailList)
                wordsInTuple = tuple(wordsInList)
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                words = []  # On the x-axis
                senders = []  # On the y-axis
                frequencies = []  # On the z-axis

                postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s ORDER BY emailaddress;"
                postgres_select_query2 = "SELECT term FROM enron.terms WHERE term IN %s ORDER BY term;"

                # Filtering only emails which are in the database people table
                cursor.execute(postgres_select_query1, (senderEmailsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    senders.append(row[0])

                if len(senders) == 0:
                    return [{'ErrorMessage': 'The sender email/s you entered were not found in the corpus'}]
                else:
                    senders.sort()  # Sorting sender emails alphabetically

                    # Filtering only terms which are in the database terms table
                    cursor.execute(postgres_select_query2, (wordsInTuple,))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        words.append(row[0])
                    if len(words) == 0:
                        return [{'ErrorMessage': 'The word/s you entered were not found in the corpus'}]
                    else:
                        words.sort()  # Sorting words alphabetically
                        # Now we have the senders list and the words list, and we have to find the frequencies

                        # This query finds how many times a particular sender mentioned each of the specified words within the specified date range
                        postgres_select_query3 = '''
                            DROP TABLE IF EXISTS termstemp;
                            DROP TABLE IF EXISTS termsfrequency;
                            CREATE TEMPORARY TABLE termstemp(term text);
                            INSERT INTO termstemp
                            SELECT t.term
                            FROM enron.terms t WHERE t.term IN %s
                            ORDER BY term;

                            CREATE TEMPORARY TABLE termsfrequency(term text,freq int);
                            INSERT INTO termsfrequency
                            SELECT et.term , SUM(et.frequency) FROM enron.emails e
                            INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                            INNER JOIN enron.people p ON e.senderid = p.personid
                            WHERE p.emailaddress = %s AND et.term IN %s
                            AND e.date BETWEEN %s AND %s
                            GROUP BY et.term
                            ORDER BY et.term;

                            SELECT tt.term, COALESCE(tf.freq,0) FROM termstemp tt LEFT JOIN termsfrequency tf ON tt.term = tf.term ORDER BY tt.term
                        '''

                        wordsTuple = tuple(words)
                        startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                        endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                        for sender in senders:
                            words_Frequencies_per_sender = []
                            cursor.execute(postgres_select_query3, (wordsTuple, sender, wordsTuple, startDate, endDate))
                            rows3 = cursor.fetchall()
                            for row in rows3:
                                words_Frequencies_per_sender.append(row[1])
                            frequencies.append(words_Frequencies_per_sender)

                        return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear),
                                'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                                'WordsXAxis': words, 'SendersYAxis': senders,
                                'FrequenciesZAxis': frequencies, 'ErrorMessage': 'None'}]
    # Returns data for a HeatMap
    class Receivers_Words_Frequency_Within_Time_Period(Resource):
        def get(self):
            args = request.args
            receiversEmailList = ast.literal_eval(
                args['receiverList'])  # This translates the string into the appropriate data type
            wordsInList = ast.literal_eval(
                args['wordList'])  # This translates the string into the appropriate data type
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

            if len(receiversEmailList) == 0:
                return [{'ErrorMessage': 'You did not enter any receiver emails!'}]
            elif len(wordsInList) == 0:
                return [{'ErrorMessage': 'You did not enter any words!'}]
            else:
                # Lemmatizing the words that user input
                wordsInList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in wordsInList]
                wordsInList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in wordsInList]

                receiversEmailsTuple = tuple(receiversEmailList)
                wordsInTuple = tuple(wordsInList)
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                words = []  # On the x-axis
                receivers = []  # On the y-axis
                frequencies = []  # On the z-axis

                postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s ORDER BY emailaddress;"
                postgres_select_query2 = "SELECT term FROM enron.terms WHERE term IN %s ORDER BY term;"

                # Filtering only emails which are in the database people table
                cursor.execute(postgres_select_query1, (receiversEmailsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    receivers.append(row[0])

                if len(receivers) == 0:
                    return [{'ErrorMessage': 'The receiver email/s you entered were not found in the corpus'}]
                else:
                    receivers.sort()  # Sorting receiver emails alphabetically

                    # Filtering only terms which are in the database terms table
                    cursor.execute(postgres_select_query2, (wordsInTuple,))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        words.append(row[0])
                    if len(words) == 0:
                        return [{'ErrorMessage': 'The word/s you entered were not found in the corpus'}]
                    else:
                        words.sort()  # Sorting words alphabetically
                        # Now we have the receivers list and the words list, and we have to find the frequencies

                        # This query finds how many times a particular receiver received each of the specified words within the specified date range
                        postgres_select_query3 = '''
                             DROP TABLE IF EXISTS termstemp;
                             DROP TABLE IF EXISTS termsfrequency;
                             CREATE TEMPORARY TABLE termstemp(term text);
                             INSERT INTO termstemp
                             SELECT t.term
                             FROM enron.terms t WHERE t.term IN %s
                             ORDER BY term;

                             CREATE TEMPORARY TABLE termsfrequency(term text,freq int);
                             INSERT INTO termsfrequency
                             SELECT et.term, SUM(et.frequency) FROM enron.emails e
                             INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                             INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid)r ON e.emailid = r.emailid
                             WHERE r.emailaddress = %s AND et.term IN %s
                             AND e.date BETWEEN %s AND %s
                             GROUP BY et.term
                             ORDER BY et.term;

                             SELECT tt.term, COALESCE(tf.freq,0) FROM termstemp tt LEFT JOIN termsfrequency tf ON tt.term = tf.term ORDER BY tt.term
                         '''

                        wordsTuple = tuple(words)
                        startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                        endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                        for receiver in receivers:
                            words_Frequencies_per_receiver = []
                            cursor.execute(postgres_select_query3,
                                           (wordsTuple, receiver, wordsTuple, startDate, endDate))
                            rows3 = cursor.fetchall()
                            for row in rows3:
                                words_Frequencies_per_receiver.append(row[1])
                            frequencies.append(words_Frequencies_per_receiver)

                        return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear),
                                'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                                'WordsXAxis': words, 'ReceiversYAxis': receivers,
                                'FrequenciesZAxis': frequencies, 'ErrorMessage': 'None'}]

    # What words did specified senders send most or specified receivers receive most within a specified date range?
    # Returns data for a HeatMap
    class Senders_Most_Popular_Words_Within_Time_Period(Resource):
        def get(self):
            args = request.args
            senderEmailList = ast.literal_eval(
                args['senderList'])  # This translates the string into the appropriate data type
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

            if len(senderEmailList) == 0:
                return [{'ErrorMessage': 'You did not enter any sender emails!'}]
            else:
                senderEmailsTuple = tuple(senderEmailList)
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                words = []  # On the x-axis
                senders = []  # On the y-axis
                frequencies = []  # On the z-axis

                postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s ORDER BY emailaddress;"

                # Filtering only emails which are in the database people table
                cursor.execute(postgres_select_query1, (senderEmailsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    senders.append(row[0])
                if len(senders) == 0:
                    return [{'ErrorMessage': 'The sender email/s you entered were not found in the corpus!'}]
                else:
                    senders.sort()  # Sorting sender emails alphabetically
                    # Now we have to find the most popular words sent by these people within the specified dates

                    # Query to find the most  popular words mentioned in the emails sent by these people within the specified date range
                    postgres_select_query2 = '''
                        SELECT et.term, SUM(et.frequency), COUNT(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        INNER JOIN enron.people p ON e.senderid = p.personid
                        WHERE p.emailaddress IN %s
                        AND e.date BETWEEN %s AND %s AND length(et.term) > 3
                        GROUP BY et.term
                        ORDER BY SUM(et.frequency) DESC, COUNT(et.frequency) DESC
                        LIMIT 50
                    '''

                    sendersTuple = tuple(senders)
                    startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                    endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                    cursor.execute(postgres_select_query2, (sendersTuple, startDate, endDate,))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        words.append(row[0])

                    if len(words) == 0:
                        return [{
                            'ErrorMessage': 'No words have been found by these senders in the specified date range!'}]

                    else:
                        words.sort()
                        wordsTuple = tuple(words)

                        # Now we have the senders list and the words list, and we have to find the frequencies

                        # This query finds how many times a particular sender mentioned the top 80 word previously found within the specified date range
                        postgres_select_query3 = '''
                            DROP TABLE IF EXISTS termstemp;
                            DROP TABLE IF EXISTS termsfrequency;
                            CREATE TEMPORARY TABLE termstemp(term text);
                            INSERT INTO termstemp
                            SELECT t.term
                            FROM enron.terms t WHERE t.term IN %s
                            ORDER BY term;

                            CREATE TEMPORARY TABLE termsfrequency(term text,freq int);
                            INSERT INTO termsfrequency
                            SELECT et.term , SUM(et.frequency) FROM enron.emails e
                            INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                            INNER JOIN enron.people p ON e.senderid = p.personid
                            WHERE p.emailaddress = %s AND et.term IN %s
                            AND e.date BETWEEN %s AND %s
                            GROUP BY et.term
                            ORDER BY et.term;

                            SELECT tt.term, COALESCE(tf.freq, 0) FROM termstemp tt LEFT JOIN termsfrequency tf ON tt.term = tf.term ORDER BY tt.term;
                        '''

                        for sender in senders:
                            words_Frequencies_per_sender = []
                            cursor.execute(postgres_select_query3,
                                           (wordsTuple, sender, wordsTuple, startDate, endDate,))
                            rows3 = cursor.fetchall()
                            for row in rows3:
                                words_Frequencies_per_sender.append(row[1])
                            frequencies.append(words_Frequencies_per_sender)

                        return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear),
                                'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                                'WordsXAxis': words, 'SendersYAxis': senders,
                                'FrequenciesZAxis': frequencies, 'ErrorMessage': 'None'}]
    # Returns data for a HeatMap
    class Receivers_Most_Popular_Words_Within_Time_Period(Resource):
        def get(self):
            args = request.args
            receiverEmailList = ast.literal_eval(
                args['receiverList'])  # This translates the string into the appropriate data type
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

            if len(receiverEmailList) == 0:
                return [{'ErrorMessage': 'You did not enter any receiver emails!'}]
            else:
                receiverEmailsTuple = tuple(receiverEmailList)
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                words = []  # On the x-axis
                receivers = []  # On the y-axis
                frequencies = []  # On the z-axis

                postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s ORDER BY emailaddress;"

                # Filtering only email addresses which are in the database's people table
                cursor.execute(postgres_select_query1, (receiverEmailsTuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    receivers.append(row[0])
                if len(receivers) == 0:
                    return [{'ErrorMessage': 'The receiver email/s you entered were not found in the corpus'}]
                else:
                    receivers.sort()  # Sorting receiver emails alphabetically
                    # Now we have to find the most popular words received by these people within the specified dates

                    # Query to find the most popular words mentioned in the emails received by these people within the specified date range
                    postgres_select_query2 = '''
                        SELECT et.term, SUM(et.frequency), COUNT(et.frequency) FROM enron.emails e
                        INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                        INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid)r ON e.emailid = r.emailid
                        WHERE r.emailaddress IN %s
                        AND e.date BETWEEN %s AND %s AND length(et.term) > 3
                        GROUP BY et.term
                        ORDER BY SUM(et.frequency) DESC, COUNT(et.frequency) DESC
                        LIMIT 50
                    '''

                    receiversTuple = tuple(receivers)
                    startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                    endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                    cursor.execute(postgres_select_query2, (receiversTuple, startDate, endDate,))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        words.append(row[0])

                    if len(words) == 0:
                        return [{
                            'ErrorMessage': 'No words sent to these receivers have been found in the specified date range'}]
                    else:
                        words.sort()
                        wordsTuple = tuple(words)

                        # Now we have the receivers list and the words list, and we have to find the frequencies

                        # This query finds how many times a particular receiver received the top 80 words (previously found) within the specified date range
                        postgres_select_query3 = '''
                            DROP TABLE IF EXISTS termstemp;
                            DROP TABLE IF EXISTS termsfrequency;
                            CREATE TEMPORARY TABLE termstemp(term text);
                            INSERT INTO termstemp
                            SELECT t.term
                            FROM enron.terms t WHERE t.term IN %s
                            ORDER BY term;

                            CREATE TEMPORARY TABLE termsfrequency(term text,freq int);
                            INSERT INTO termsfrequency
                            SELECT et.term, SUM(et.frequency)  FROM enron.emails e
                            INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                            INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid)r ON e.emailid = r.emailid
                            WHERE r.emailaddress = %s AND et.term IN %s
                            AND e.date BETWEEN %s AND %s
                            GROUP BY et.term
                            ORDER BY et.term;

                            SELECT tt.term, COALESCE(tf.freq, 0) FROM termstemp tt LEFT JOIN termsfrequency tf ON tt.term = tf.term ORDER BY tt.term;
                        '''

                        for receiver in receivers:
                            words_Frequencies_Per_Receiver = []
                            cursor.execute(postgres_select_query3,
                                           (wordsTuple, receiver, wordsTuple, startDate, endDate,))
                            rows3 = cursor.fetchall()
                            for row in rows3:
                                words_Frequencies_Per_Receiver.append(row[1])
                            frequencies.append(words_Frequencies_Per_Receiver)

                        return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear),
                                'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                                'WordsXAxis': words, 'ReceiversYAxis': receivers,
                                'FrequenciesZAxis': frequencies, 'ErrorMessage': 'None'}]



    # CORRESPONDENCE HEATMAPS(SENDERS VS RECEIVERS)
    # Basic
    # Returns data for a HeatMap
    class correspondence_Heatmap_overall(Resource):
        def get(self):
            args = request.args
            emails_list = ast.literal_eval(
                args['senderList'])  # This translates the string into the appropriate data type



            if len(emails_list) == 0:
                return [{'ErrorMessage': 'You did  not enter any sender emails!'}]
            else:
                emails_tuple = tuple(emails_list)
                # These will be passed to the plotly heatmap
                senderemails = []  # On y-axis
                recieveremails = []  # On x-axis
                numberOfEmailsSent = []  # On z-axis

                # Filtering out only email addresses which are in the database's people table
                postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s"
                cursor.execute(postgres_select_query1, (emails_tuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    senderemails.append(row[0])

                if len(senderemails) == 0:
                    return [{'ErrorMessage': 'No sender email you entered was found in the corpus!'}]
                else:
                    senderemails.sort()  # Sorting the sender emails alphabetically

                    # This is the query to get the top 4 recipients of a particular sender
                    postgres_select_query2 = '''
                        SELECT p1.emailaddress AS sender, r.emailaddress AS recievers, COUNT(r.emailaddress) AS emailssent
                        FROM enron.emails e 
                        INNER JOIN enron.people p1 ON e.senderid = p1.personid
                        INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                        WHERE p1.emailaddress = %s
                        GROUP BY p1.emailaddress, r.emailaddress
                        ORDER BY COUNT(r.emailaddress) DESC
                        LIMIT 4;
                    '''

                    # Populating the recievers list (this will be on the x-axis)
                    for sender in senderemails:
                        cursor.execute(postgres_select_query2, (sender,))
                        rows2 = cursor.fetchall()
                        for row in rows2:
                            if "'" not in row[1]:
                                recieveremails.append(row[1])

                    # Removing duplicate receivers and ordering them alphabetically
                    recieveremailsSet = set(recieveremails)
                    recieveremails = list(recieveremailsSet)
                    recieveremails.sort()
                    receiverEmailsTuple = tuple(recieveremails)

                    # This query is to get the number of emails a sender has sent each receiver
                    postgres_select_query3 = '''
                        DROP TABLE IF EXISTS receiverEmails;
                        DROP TABLE IF EXISTS receiversFrequencyOFEmails;
                        CREATE TEMPORARY TABLE receiverEmails(emailaddress text);
                        INSERT INTO receiverEmails
                        SELECT p.emailaddress
                        FROM enron.people p WHERE p.emailaddress IN %s
                        ORDER BY p.emailaddress;

                        CREATE TEMPORARY TABLE receiversFrequencyOFEmails(emailaddress text, frequency int);
                        INSERT INTO receiversFrequencyOFEmails
                        SELECT r.emailaddress, COUNT(r.emailaddress) as receiver FROM enron.emails e
                        INNER JOIN enron.people p1 ON e.senderid = p1.personid
                        INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                        WHERE p1.emailaddress = %s AND r.emailaddress IN %s
                        GROUP BY r.emailaddress
                        ORDER BY r.emailaddress;

                        SELECT re.emailaddress, COALESCE(rfoe.frequency,0) FROM receiverEmails re LEFT JOIN receiversFrequencyOFEmails rfoe ON re.emailaddress = rfoe.emailaddress ORDER BY re.emailaddress;
                    '''

                    for sender in senderemails:
                        senderFrequencyList = []
                        cursor.execute(postgres_select_query3, (receiverEmailsTuple, sender, receiverEmailsTuple,))
                        rows3 = cursor.fetchall()
                        for row in rows3:
                            senderFrequencyList.append(row[1])
                        numberOfEmailsSent.append(senderFrequencyList)

                    return [{'ReceiversXAxis': recieveremails, 'SendersYAxis': senderemails,
                            'FrequenciesZAxis': numberOfEmailsSent, 'ErrorMessage': 'None'}]

    # Within specified date range
    # Returns data for a HeatMap
    class correspondence_Heatmap_Within_Time_Period(Resource):
        def get(self):
            args = request.args
            emails_list = ast.literal_eval(
                args['senderList'])  # This translates the string into the appropriate data type
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            if len(emails_list) == 0:
                return [{'ErrorMessage': 'You did not enter any sender emails!'}]
            else:
                emails_tuple = tuple(emails_list)
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October',
                          'November', 'December']
                startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
            
                # These will be passed to the plotly heatmap
                senderemails = []  # On y-axis
                recieveremails = []  # On x-axis
                numberOfEmailsSent = []  # On z-axis

                # Filtering out only email addresses which are in the database's people table
                postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s"
                cursor.execute(postgres_select_query1, (emails_tuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    senderemails.append(row[0])

                if len(senderemails) == 0:
                    return [{'ErrorMessage': 'No sender email you entered was found in the corpus!'}]
                else:
                    senderemails.sort()  # Sorting the sender emails alphabetically

                    # This is the query to get the top 4 recipients of a particular sender within the specified time period
                    postgres_select_query2 = '''
                        SELECT r.emailaddress, COUNT(r.emailaddress) FROM enron.emails e 
                        INNER JOIN enron.people p ON e.senderid = p.personid
                        INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                        WHERE p.emailaddress = %s AND e.date BETWEEN %s AND %s
                        GROUP BY r.emailaddress
                        ORDER BY COUNT(r.emailaddress) DESC
                        LIMIT 4;
                    '''

                    # Populating the recievers list (this will be on the x-axis)
                    for sender in senderemails:
                        cursor.execute(postgres_select_query2, (sender, startDate, endDate,))
                        rows2 = cursor.fetchall()
                        for row in rows2:
                            if "'" not in row[0]:
                                recieveremails.append(row[0])

                    # Removing duplicate receivers and ordering them alphabetically
                    recieveremailsSet = set(recieveremails)
                    recieveremails = list(recieveremailsSet)
                    recieveremails.sort()
                    receiverEmailsTuple = tuple(recieveremails)

                    # This query is to get the number of emails a sender has sent each receiver within the specified time period
                    postgres_select_query3 = '''
                        DROP TABLE IF EXISTS receiverEmails;
                        DROP TABLE IF EXISTS receiversFrequencyOFEmails;
                        CREATE TEMPORARY TABLE receiverEmails(emailaddress text);
                        INSERT INTO receiverEmails
                        SELECT p.emailaddress
                        FROM enron.people p WHERE p.emailaddress IN %s
                        ORDER BY p.emailaddress;

                        CREATE TEMPORARY TABLE receiversFrequencyOFEmails(emailaddress text, frequency int);
                        INSERT INTO receiversFrequencyOFEmails
                        SELECT r.emailaddress, COUNT(r.emailaddress) as receiver FROM enron.emails e
                        INNER JOIN enron.people p1 ON e.senderid = p1.personid
                        INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                        WHERE p1.emailaddress = %s AND r.emailaddress IN %s
                        AND e.date BETWEEN %s AND %s
                        GROUP BY r.emailaddress
                        ORDER BY r.emailaddress;

                        SELECT re.emailaddress, COALESCE(rfoe.frequency,0) FROM receiverEmails re LEFT JOIN receiversFrequencyOFEmails rfoe ON re.emailaddress = rfoe.emailaddress ORDER BY re.emailaddress;
                    '''

                    for sender in senderemails:
                        senderFrequencyList = []
                        cursor.execute(postgres_select_query3,
                                       (receiverEmailsTuple, sender, receiverEmailsTuple, startDate, endDate,))
                        rows3 = cursor.fetchall()
                        for row in rows3:
                            senderFrequencyList.append(row[1])
                        numberOfEmailsSent.append(senderFrequencyList)

                    return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear),
                            'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                            'ReceiversXAxis': recieveremails, 'SendersYAxis': senderemails,
                            'FrequenciesZAxis': numberOfEmailsSent, 'ErrorMessage': 'None'}]

    # With particular terms
    # Returns data for a HeatMap
    class correspondence_Heatmap_With_Particular_Terms(Resource):
        def get(self):
            args = request.args
            emails_list = ast.literal_eval(
                args['senderList'])  # This translates the string into the appropriate data type
            termsInList = ast.literal_eval(
                args['wordList'])  # This translates the string into the appropriate data type

            if len(emails_list) == 0:
                return [{'ErrorMessage': 'You did not enter any sender emails!'}]
            elif len(termsInList) == 0:
                return [{'ErrorMessage': 'You did not enter any terms!'}]
            else:
                # Lemmatizing the words that user input
                termsInList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in termsInList]
                termsInList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in termsInList]
                termsInList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in termsInList]
                termsInList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in termsInList]

                emails_tuple = tuple(emails_list)
                termsInTuple = tuple(termsInList)
                termsList = []
                # These will be passed to the plotly heatmap
                senderemails = []  # On y-axis
                recieveremails = []  # On x-axis
                numberOfEmailsSent = []  # On z-axis

                # Filtering out only email addresses which are in the database's people table
                postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s"
                cursor.execute(postgres_select_query1, (emails_tuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    senderemails.append(row[0])

                if len(senderemails) == 0:
                    return [{'ErrorMessage': 'No sender email you entered was found in the corpus!'}]
                else:
                    senderemails.sort()  # Sorting the sender emails alphabetically

                    # Filtering out only terms which are in the database's terms table
                    postgres_select_query2 = "SELECT term FROM enron.terms WHERE term IN %s"
                    cursor.execute(postgres_select_query2, (termsInTuple,))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        termsList.append(row[0])

                    if len(termsList) == 0:
                        return [{'ErrorMessage': 'No term you entered was found in the corpus!'}]
                    else:
                        termsList.sort()  # Sorting the sender emails alphabetically
                        termsTuple = tuple(termsList)

                        # This is the query to find out who each of the specified senders sent any of the specified words the most to (top 4 recipients)
                        postgres_select_query3 = '''
                            SELECT derivedTable.receiver, COUNT(derivedTable.receiver)
                            FROM (
                            SELECT DISTINCT e.emailid, p.emailaddress as sender , r.emailaddress as receiver FROM enron.emails e
                            INNER JOIN enron.people p ON e.senderid = p.personid
                            INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                            INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                            WHERE p.emailaddress = %s AND et.term IN %s
                            ) AS derivedTable
                            GROUP BY derivedTable.receiver
                            ORDER BY COUNT(derivedTable.receiver) DESC
                            LIMIT 4;
                        '''

                        # Populating the receivers list (this will be on the x-axis)
                        for sender in senderemails:
                            cursor.execute(postgres_select_query3, (sender, termsTuple,))
                            rows3 = cursor.fetchall()
                            for row in rows3:
                                if "'" not in row[0]:
                                    recieveremails.append(row[0])

                        # Removing duplicate receivers and ordering them alphabetically
                        recieveremailsSet = set(recieveremails)
                        recieveremails = list(recieveremailsSet)
                        recieveremails.sort()
                        receiverEmailsTuple = tuple(recieveremails)

                        # This query is to get the number of emails a sender has sent each receiver with any of the specified words
                        postgres_select_query4 = '''
                            DROP TABLE IF EXISTS receiverEmails;
                            DROP TABLE IF EXISTS receiversFrequencyOFEmails;
                            CREATE TEMPORARY TABLE receiverEmails(emailaddress text);
                            INSERT INTO receiverEmails
                            SELECT p.emailaddress
                            FROM enron.people p WHERE p.emailaddress IN %s
                            ORDER BY p.emailaddress;

                            CREATE TEMPORARY TABLE receiversFrequencyOFEmails(emailaddress text, frequency int);
                            INSERT INTO receiversFrequencyOFEmails
                            SELECT derivedTable.receiver, COUNT(derivedTable.receiver)
                            FROM (
                            SELECT DISTINCT e.emailid, p.emailaddress as sender , r.emailaddress as receiver FROM enron.emails e
                            INNER JOIN enron.people p ON e.senderid = p.personid
                            INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                            INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                            WHERE p.emailaddress = %s AND et.term IN %s
                            AND r.emailaddress IN %s
                            ) AS derivedTable
                            GROUP BY derivedTable.receiver
                            ORDER BY derivedTable.receiver;

                            SELECT re.emailaddress, COALESCE(rfoe.frequency,0) FROM receiverEmails re LEFT JOIN receiversFrequencyOFEmails rfoe ON re.emailaddress = rfoe.emailaddress ORDER BY re.emailaddress;
                        '''

                        for sender in senderemails:
                            senderFrequencyList = []
                            cursor.execute(postgres_select_query4,
                                           (receiverEmailsTuple, sender, termsTuple, receiverEmailsTuple,))
                            rows3 = cursor.fetchall()
                            for row in rows3:
                                senderFrequencyList.append(row[1])
                            numberOfEmailsSent.append(senderFrequencyList)

                        return [{'WordsInput': termsList, 'ReceiversXAxis': recieveremails,
                                'SendersYAxis': senderemails, 'FrequenciesZAxis': numberOfEmailsSent,
                                'ErrorMessage': 'None'}]

    # With particular terms within specified date range
    # Returns data for a HeatMap
    class correspondence_Heatmap_With_Particular_Terms_Within_Time_Period(Resource):
        def get(self):
            args = request.args
            emails_list = ast.literal_eval(args['senderList']) #This translates the string into the appropriate data type
            termsInList = ast.literal_eval(args['wordList']) #This translates the string into the appropriate data type
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            if len(emails_list) == 0:
                return [{'ErrorMessage' : 'You did not enter any sender emails!'}]
            elif len(termsInList) == 0:
                return [{'ErrorMessage': 'You did not enter any terms!'}]
            else:
                # Lemmatizing the words that user input
                termsInList = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in termsInList]
                termsInList = [lemmatizer.lemmatize(word, wordnet.VERB) for word in termsInList]
                termsInList = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in termsInList]
                termsInList = [lemmatizer.lemmatize(word, wordnet.ADV) for word in termsInList]

                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                emails_tuple = tuple(emails_list)
                termsInTuple = tuple(termsInList)
                termsList = []
                # These will be passed to the plotly heatmap
                senderemails = []  # On y-axis
                recieveremails = []  # On x-axis
                numberOfEmailsSent = []  # On z-axis

                # Filtering out only email addresses which are in the database's people table
                postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s"
                cursor.execute(postgres_select_query1, (emails_tuple,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    senderemails.append(row[0])

                if len(senderemails) == 0:
                    return [{'ErrorMessage': 'No sender email you entered was found in the corpus!'}]
                else:
                    senderemails.sort()  # Sorting the sender emails alphabetically

                    # Filtering out only terms which are in the database's terms table
                    postgres_select_query2 = "SELECT term FROM enron.terms WHERE term IN %s"
                    cursor.execute(postgres_select_query2, (termsInTuple,))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        termsList.append(row[0])

                    if len(termsList) == 0:
                        return [{'ErrorMessage': 'No term you entered was found in the corpus!'}]
                    else:
                        termsList.sort()  # Sorting the sender emails alphabetically
                        termsTuple = tuple(termsList)

                        # This is the query to find out who each of the specified senders sent any of the specified words the most to, within a particular date range(top 4 recipients)
                        postgres_select_query3 = '''
                            SELECT derivedTable.receiver, COUNT(derivedTable.receiver)
                            FROM (
                            SELECT DISTINCT e.emailid, p.emailaddress as sender , r.emailaddress as receiver FROM enron.emails e
                            INNER JOIN enron.people p ON e.senderid = p.personid
                            INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                            INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                            WHERE p.emailaddress = %s AND et.term IN %s
                            AND e.date BETWEEN %s AND %s
                            ) AS derivedTable
                            GROUP BY derivedTable.receiver
                            ORDER BY COUNT(derivedTable.receiver) DESC
                            LIMIT 4;
                        '''

                        # Populating the receivers list (this will be on the x-axis)
                        for sender in senderemails:
                            cursor.execute(postgres_select_query3, (sender, termsTuple, startDate, endDate,))
                            rows3 = cursor.fetchall()
                            for row in rows3:
                                if "'" not in row[0]:
                                    recieveremails.append(row[0])

                        # Removing duplicate receivers and ordering them alphabetically
                        recieveremailsSet = set(recieveremails)
                        recieveremails = list(recieveremailsSet)
                        recieveremails.sort()
                        receiverEmailsTuple = tuple(recieveremails)

                        # This query is to get the number of emails a sender has sent each receiver with any of the specified words
                        postgres_select_query4 = '''
                            DROP TABLE IF EXISTS receiverEmails;
                            DROP TABLE IF EXISTS receiversFrequencyOFEmails;
                            CREATE TEMPORARY TABLE receiverEmails(emailaddress text);
                            INSERT INTO receiverEmails
                            SELECT p.emailaddress
                            FROM enron.people p WHERE p.emailaddress IN %s
                            ORDER BY p.emailaddress;

                            CREATE TEMPORARY TABLE receiversFrequencyOFEmails(emailaddress text, frequency int);
                            INSERT INTO receiversFrequencyOFEmails
                            SELECT derivedTable.receiver, COUNT(derivedTable.receiver)
                            FROM (
                            SELECT DISTINCT e.emailid, p.emailaddress as sender , r.emailaddress as receiver FROM enron.emails e
                            INNER JOIN enron.people p ON e.senderid = p.personid
                            INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                            INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                            WHERE p.emailaddress = %s AND et.term IN %s
                            AND r.emailaddress IN %s
                            AND e.date BETWEEN %s AND %s
                            ) AS derivedTable
                            GROUP BY derivedTable.receiver
                            ORDER BY derivedTable.receiver;

                            SELECT re.emailaddress, COALESCE(rfoe.frequency,0) FROM receiverEmails re LEFT JOIN receiversFrequencyOFEmails rfoe ON re.emailaddress = rfoe.emailaddress ORDER BY re.emailaddress;
                        '''

                        for sender in senderemails:
                            senderFrequencyList = []
                            cursor.execute(postgres_select_query4, (
                                receiverEmailsTuple, sender, termsTuple, receiverEmailsTuple, startDate, endDate,))
                            rows3 = cursor.fetchall()
                            for row in rows3:
                                senderFrequencyList.append(row[1])
                            numberOfEmailsSent.append(senderFrequencyList)

                        return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear), 'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear), 'WordsInput' : termsList, 'ReceiversXAxis': recieveremails, 'SendersYAxis': senderemails, 'FrequenciesZAxis': numberOfEmailsSent, 'ErrorMessage' : 'None'}]



    # CALENDAR HEATMAPS(VOLUME OF EMAILS SENT)
    # Basic
    # Returns data for a HeatMap
    class calendar_heatmap_for_year(Resource):
        def get(self):
            args = request.args
            year = int(args['year'])

            if year < 1900 or year > 2019:
                return [{'ErrorMessage' : 'The year you entered is invalid!'}]
            else:
                startDate = str(year) + "-01-01"
                endDate = str(year) + "-12-31"
                leapYrs = [1996, 2000, 2004, 2008]
                daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

                query = "SELECT date(gs.*), EXTRACT(dow FROM DATE(gs.*)), COUNT(emails.emailid) FROM generate_series(%s::timestamp, %s::timestamp, '1day') AS gs" \
                        " LEFT JOIN enron.emails ON date(gs.*) = date(emails.date) GROUP BY gs.*, date(emails.date)"
                cursor.execute(query, (startDate, endDate,))
                records = cursor.fetchall()

                numOfEmails = []
                xdata = []
                ydata = []

                for row in records:
                    sDate = str(row[0])
                    numOfEmails.append(row[2])
                    if sDate == startDate:
                        dow1 = int(row[1])
                    elif sDate == endDate:
                        dow2 = int(row[1])

                for i in range(7 - dow1):
                    xdata.append(1)

                for j in range(51):
                    for k in range(7):
                        xdata.append(j + 2)

                if year in leapYrs:
                    daysInMonth = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                    for k in range(7):
                        xdata.append(53)

                    for l in range(1 + dow2):
                        xdata.append(54)

                else:
                    for l in range(1 + dow2):
                        xdata.append(53)

                for i in range(7):
                    if i >= dow1:
                        ydata.append(i + 1)

                for j in range(51):
                    for k in range(7):
                        ydata.append(k + 1)

                if year in leapYrs:
                    for k in range(7):
                        ydata.append(k + 1)

                for l in range(7):
                    if l + 1 <= dow2 + 1:
                        ydata.append(l + 1)
                    else:
                        break

                hovertext = []

                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                k = 0

                for i in range(len(months)):
                    for j in range(daysInMonth[i]):
                        s = str(j + 1)
                        s += " "
                        s += months[i]
                        s += " - "
                        s += str(numOfEmails[k])
                        k += 1
                        hovertext.append(s)

                return [{'YearInput' : year, 'MonthsInYearXAxis' : xdata, 'DaysOfWeekYAxis' : ydata, 'VolumeOfEmailsSentInYearZAxis' : numOfEmails, 'HoverText' : hovertext, 'ErrorMessage' : 'None'}]

    # With respect to specified people
    # Returns data for a HeatMap
    class calendar_heatmap_for_year_with_particular_senders(Resource):
        def get(self):
            args = request.args
            year = int(args['year'])
            sendersListIn = ast.literal_eval(args['senderList'])  # This translates the string into the appropriate data type

            if year < 1900 or year > 2019:
                return [{'ErrorMessage' : 'The year you entered is invalid!'}]
            elif len(sendersListIn) == 0:
                return [{'ErrorMessage': 'You did not enter any senders!'}]
            else:
                startDate = str(year) + "-01-01"
                endDate = str(year) + "-12-31"
                sendersTupleIn = tuple(sendersListIn)
                senders = []
                leapYrs = [1996, 2000, 2004, 2008]
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October',
                          'November', 'December']
                daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

                # Filtering out only email addresses which are in the database's people table
                postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s"
                cursor.execute(postgres_select_query1, (sendersTupleIn,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    senders.append(row[0])

                if len(senders) == 0:
                    return [{'ErrorMessage': 'No sender email you entered was found in the corpus!'}]
                else:
                    senders.sort()
                    sendersTuple = tuple(senders)

                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS daysInYears; 
                        DROP TABLE IF EXISTS emailsPerDay; 
                        CREATE TEMPORARY TABLE daysInYears(day date); 
                        INSERT INTO daysInYears
                        SELECT * FROM generate_series(%s::timestamp, %s::timestamp,'1day');
                        CREATE TEMPORARY TABLE emailsPerDay(day date,freq int); 
                        INSERT INTO emailsPerDay 
                        SELECT date(e.date), COUNT(date(e.date)) FROM enron.emails e
                        INNER JOIN enron.people p ON e.senderid = p.personid 
                        WHERE extract(year FROM e.date) = %s AND  p.emailaddress IN %s
                        GROUP BY date(e.date) ORDER BY date(e.date);
                        SELECT date(dY.day), COALESCE(eD.freq, 0), EXTRACT(dow FROM dy.day) FROM daysInYears dy LEFT JOIN emailsPerDay eD ON dy.day = eD.day ORDER by dy.day;
                    '''

                    cursor.execute(postgres_select_query2, (startDate, endDate, year, sendersTuple,))
                    records = cursor.fetchall()

                    numOfEmails = []
                    xdata = []
                    ydata = []

                    for row in records:
                        sDate = str(row[0])
                        numOfEmails.append(row[1])
                        if sDate == startDate:
                            dow1 = int(row[2])
                        elif sDate == endDate:
                            dow2 = int(row[2])

                    for i in range(7 - dow1):
                        xdata.append(1)

                    for j in range(51):
                        for k in range(7):
                            xdata.append(j + 2)

                    if year in leapYrs:
                        daysInMonth = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                        for k in range(7):
                            xdata.append(53)

                        for l in range(1 + dow2):
                            xdata.append(54)

                    else:
                        for l in range(1 + dow2):
                            xdata.append(53)

                    for i in range(7):
                        if i >= dow1:
                            ydata.append(i + 1)

                    for j in range(51):
                        for k in range(7):
                            ydata.append(k + 1)

                    if year in leapYrs:
                        for k in range(7):
                            ydata.append(k + 1)

                    for l in range(7):
                        if l + 1 <= dow2 + 1:
                            ydata.append(l + 1)
                        else:
                            break

                    hovertext = []

                    k = 0

                    for i in range(len(months)):
                        for j in range(daysInMonth[i]):
                            s = str(j + 1)
                            s += " "
                            s += months[i]
                            s += " - "
                            s += str(numOfEmails[k])
                            k += 1
                            hovertext.append(s)

                    return [{'YearInput': year, 'SendersInput' : senders, 'MonthsInYearXAxis': xdata, 'DaysOfWeekYAxis': ydata, 'VolumeOfEmailsSentInYearZAxis': numOfEmails, 'HoverText' : hovertext, 'ErrorMessage' : 'None'}]
    # Returns data for a HeatMap
    class calendar_heatmap_for_year_with_particular_receivers(Resource):
        def get(self):
            args = request.args
            year = int(args['year'])
            receiversInList = ast.literal_eval(args['receiverList'])  # This translates the string into the appropriate data type

            if year < 1900 or year > 2019:
                return [{'ErrorMessage' : 'The year you entered is invalid!'}]
            elif len(receiversInList) == 0:
                return [{'ErrorMessage': 'You did not enter any senders!'}]
            else:
                startDate = str(year) + "-01-01"
                endDate = str(year) + "-12-31"
                receiversTupleIn = tuple(receiversInList)
                receivers = []
                leapYrs = [1996, 2000, 2004, 2008]
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October', 'November', 'December']
                daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

                # Filtering out only email addresses which are in the database's people table
                postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s"
                cursor.execute(postgres_select_query1, (receiversTupleIn,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    receivers.append(row[0])

                if len(receivers) == 0:
                    return [{'ErrorMessage': 'No receiver email you entered was found in the corpus!'}]
                else:
                    receivers.sort()
                    receiversTuple = tuple(receivers)

                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS daysInYears; 
                        DROP TABLE IF EXISTS emailsPerDay;
                        CREATE TEMPORARY TABLE daysInYears(day date); 
                        INSERT INTO daysInYears
                        SELECT * FROM generate_series(%s::timestamp, %s::timestamp,'1day');
                        CREATE TEMPORARY TABLE emailsPerDay(day date,freq int); 
                        INSERT INTO emailsPerDay
                        SELECT date(e.date), COUNT(date(e.date)) FROM enron.emails e
                        INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                        WHERE extract(year FROM e.date) = %s AND  r.emailaddress IN %s 
                        GROUP BY date(e.date) ORDER BY date(e.date);
                        SELECT date(dY.day), COALESCE(eD.freq, 0), EXTRACT(dow FROM dy.day) FROM daysInYears dy LEFT JOIN emailsPerDay eD ON dy.day = eD.day ORDER by dy.day;
                    '''

                    cursor.execute(postgres_select_query2, (startDate, endDate, year, receiversTuple,))
                    records = cursor.fetchall()

                    numOfEmails = []
                    xdata = []
                    ydata = []

                    for row in records:
                        sDate = str(row[0])
                        numOfEmails.append(row[1])
                        if sDate == startDate:
                            dow1 = int(row[2])
                        elif sDate == endDate:
                            dow2 = int(row[2])

                    for i in range(7 - dow1):
                        xdata.append(1)

                    for j in range(51):
                        for k in range(7):
                            xdata.append(j + 2)

                    if year in leapYrs:
                        daysInMonth = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                        for k in range(7):
                            xdata.append(53)

                        for l in range(1 + dow2):
                            xdata.append(54)

                    else:
                        for l in range(1 + dow2):
                            xdata.append(53)

                    for i in range(7):
                        if i >= dow1:
                            ydata.append(i + 1)

                    for j in range(51):
                        for k in range(7):
                            ydata.append(k + 1)

                    if year in leapYrs:
                        for k in range(7):
                            ydata.append(k + 1)

                    for l in range(7):
                        if l + 1 <= dow2 + 1:
                            ydata.append(l + 1)
                        else:
                            break

                    hovertext = []

                    k = 0

                    for i in range(len(months)):
                        for j in range(daysInMonth[i]):
                            s = str(j + 1)
                            s += " "
                            s += months[i]
                            s += " - "
                            s += str(numOfEmails[k])
                            k += 1
                            hovertext.append(s)

                    return [{'YearInput': year, 'ReceiversInput' : receivers, 'MonthsInYearXAxis': xdata, 'DaysOfWeekYAxis': ydata, 'VolumeOfEmailsSentInYearZAxis': numOfEmails, 'HoverText' : hovertext, 'ErrorMessage' : 'None'}]

    # With respect to specified terms
    # Returns data for a HeatMap
    class calendar_heatmap_for_year_with_particular_terms(Resource):
        def get(self):
            args = request.args
            year = int(args['year'])
            termsListIn = ast.literal_eval(args['wordList'])  # This translates the string into the appropriate data type

            if year < 1900 or year > 2019:
                return [{'ErrorMessage' : 'The year you entered is invalid!'}]
            elif len(termsListIn) == 0:
                return [{'ErrorMessage': 'You did not enter any terms!'}]
            else:
                # Lemmatizing the words that user input
                termsListIn = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.VERB) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.ADV) for word in termsListIn]

                startDate = str(year) + "-01-01"
                endDate = str(year) + "-12-31"
                termsTupleIn = tuple(termsListIn)
                terms = []
                leapYrs = [1996, 2000, 2004, 2008]
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

                # Filtering out only terms which are in the database's terms table
                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"
                cursor.execute(postgres_select_query1, (termsTupleIn,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    terms.append(row[0])

                if len(terms) == 0:
                    return [{'ErrorMessage': 'No term you entered was found in the corpus!'}]
                else:
                    terms.sort()
                    termsTuple = tuple(terms)

                    # This query gets the amount of emails sent per day of a particular year which contains one or more of the specified terms.
                    postgres_select_query2 = '''
                        DROP TABLE IF EXISTS daysInYear; 
                        DROP TABLE IF EXISTS emailsPerDay;

                        CREATE TEMPORARY TABLE daysInYear(day date); 
                        INSERT INTO daysInYear
                        SELECT * FROM generate_series(%s::timestamp, %s::timestamp,'1day');

                        CREATE TEMPORARY TABLE emailsPerDay(day date,freq int); 
                        INSERT INTO emailsPerDay
                        SELECT date(derivedTable.date), COUNT(date(derivedTable.date)) FROM (
                            SELECT DISTINCT e.emailid as emailid, e.date as date FROM enron.emails e
                            INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                            WHERE extract(year FROM e.date) = %s AND et.term IN %s
                        ) derivedTable
                        GROUP BY date(derivedTable.date) 
                        ORDER BY date(derivedTable.date);

                        SELECT date(dY.day), COALESCE(eD.freq, 0), EXTRACT(dow FROM dy.day) FROM daysInYear dy LEFT JOIN emailsPerDay eD ON dy.day = eD.day ORDER by dy.day;  
                    '''

                    cursor.execute(postgres_select_query2, (startDate, endDate, year, termsTuple,))
                    records = cursor.fetchall()

                    numOfEmails = []
                    xdata = []
                    ydata = []

                    for row in records:
                        sDate = str(row[0])
                        numOfEmails.append(row[1])
                        if sDate == startDate:
                            dow1 = int(row[2])
                        elif sDate == endDate:
                            dow2 = int(row[2])

                    for i in range(7 - dow1):
                        xdata.append(1)

                    for j in range(51):
                        for k in range(7):
                            xdata.append(j + 2)

                    if year in leapYrs:
                        daysInMonth = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                        for k in range(7):
                            xdata.append(53)

                        for l in range(1 + dow2):
                            xdata.append(54)

                    else:
                        for l in range(1 + dow2):
                            xdata.append(53)

                    for i in range(7):
                        if i >= dow1:
                            ydata.append(i + 1)

                    for j in range(51):
                        for k in range(7):
                            ydata.append(k + 1)

                    if year in leapYrs:
                        for k in range(7):
                            ydata.append(k + 1)

                    for l in range(7):
                        if l + 1 <= dow2 + 1:
                            ydata.append(l + 1)
                        else:
                            break

                    hovertext = []

                    k = 0

                    for i in range(len(months)):
                        for j in range(daysInMonth[i]):
                            s = str(j + 1)
                            s += " "
                            s += months[i]
                            s += " - "
                            s += str(numOfEmails[k])
                            k += 1
                            hovertext.append(s)

                    return [{'YearInput': year, 'WordsInput' : terms, 'MonthsInYearXAxis': xdata, 'DaysOfWeekYAxis': ydata, 'VolumeOfEmailsSentInYearZAxis': numOfEmails, 'HoverText' : hovertext, 'ErrorMessage' : 'None'}]

    # With respect to specified people and specified terms
    # Returns data for a HeatMap
    class calendar_heatmap_for_year_with_particular_senders_receivers_terms(Resource):
        def get(self):
            args = request.args
            year = int(args['year'])
            sendersListIn = ast.literal_eval(args['senderList'])  # This translates the string into the appropriate data type
            receiversListIn = ast.literal_eval(args['receiverList'])  # This translates the string into the appropriate data type
            termsListIn = ast.literal_eval(args['wordList'])  # This translates the string into the appropriate data type

            if year < 1900 or year > 2019:
                return [{'ErrorMessage' : 'The year you entered is invalid!'}]
            elif len(termsListIn) == 0:
                return [{'ErrorMessage': 'You did not enter any terms!'}]
            elif len(sendersListIn) == 0:
                return [{'ErrorMessage': 'You did not enter any sender emails!'}]
            elif len(receiversListIn) == 0:
                return [{'ErrorMessage': 'You did not enter any receiver emails!'}]
            else:
                # Lemmatizing the words that user input
                termsListIn = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.VERB) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in termsListIn]
                termsListIn = [lemmatizer.lemmatize(word, wordnet.ADV) for word in termsListIn]

                startDate = str(year) + "-01-01"
                endDate = str(year) + "-12-31"
                termsTupleIn = tuple(termsListIn)
                terms = []
                sendersTupleIn = tuple(sendersListIn)
                senders = []
                receiversTupleIn = tuple(receiversListIn)
                receivers = []
                leapYrs = [1996, 2000, 2004, 2008]
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                          'October',
                          'November', 'December']
                daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

                postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

                # Filtering out only terms which are in the database's terms table
                cursor.execute(postgres_select_query1, (termsTupleIn,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    terms.append(row[0])

                if len(terms) == 0:
                    return [{'ErrorMessage': 'No term you entered was found in the corpus!'}]
                else:
                    terms.sort()
                    termsTuple = tuple(terms)

                    postgres_select_query2 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s"

                    # Filtering out only sender emails which are in the database's people table
                    cursor.execute(postgres_select_query2, (sendersTupleIn,))
                    rows2 = cursor.fetchall()
                    for row in rows2:
                        senders.append(row[0])

                    if len(senders) == 0:
                        return [{'ErrorMessage': 'No sender email you entered was found in the corpus!'}]
                    else:
                        senders.sort()
                        sendersTuple = tuple(senders)

                        # Filtering out only receiver emails which are in the database's people table
                        cursor.execute(postgres_select_query2, (receiversTupleIn,))
                        rows3 = cursor.fetchall()
                        for row in rows3:
                            receivers.append(row[0])

                        if len(receivers) == 0:
                            return [{'ErrorMessage': 'No receiver email you entered was found in the corpus!'}]
                        else:
                            receivers.sort()
                            receiversTuple = tuple(receivers)

                            # This query gets the amount of emails sent by a sender in the senders lists and received by one or more receivers
                            # in the receivers list per day in a particular year which contains one or more of the specified terms.
                            postgres_select_query3 = '''
                                DROP TABLE IF EXISTS daysInYear; 
                                DROP TABLE IF EXISTS emailsPerDay;

                                CREATE TEMPORARY TABLE daysInYear(day date); 
                                INSERT INTO daysInYear
                                SELECT * FROM generate_series(%s::timestamp, %s::timestamp,'1day');

                                CREATE TEMPORARY TABLE emailsPerDay(day date,freq int); 
                                INSERT INTO emailsPerDay
                                SELECT date(derivedTable.date), COUNT(date(derivedTable.date)) FROM(
                                SELECT DISTINCT e.emailid as emailid, e.date as date FROM enron.emails e
                                INNER JOIN (enron.emailterms INNER JOIN enron.terms ON emailterms.termid = terms.termid)et ON e.emailid = et.emailid
                                INNER JOIN enron.people AS p ON p.personid = e.senderid
                                INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                                WHERE extract(year FROM e.date) = %s AND et.term IN %s
                                AND p.emailaddress IN %s
                                AND  r.emailaddress IN %s
                                ORDER BY e.emailid)derivedTable
                                GROUP BY date(derivedTable.date) 
                                ORDER BY date(derivedTable.date);

                                SELECT date(dY.day), COALESCE(eD.freq, 0), EXTRACT(dow FROM dy.day) FROM daysInYear dy LEFT JOIN emailsPerDay eD ON dy.day = eD.day ORDER by dy.day;  
                            '''

                            cursor.execute(postgres_select_query3,
                                           (startDate, endDate, year, termsTuple, sendersTupleIn, receiversTupleIn,))
                            records = cursor.fetchall()

                            numOfEmails = []
                            xdata = []
                            ydata = []

                            for row in records:
                                sDate = str(row[0])
                                numOfEmails.append(row[1])
                                if sDate == startDate:
                                    dow1 = int(row[2])
                                elif sDate == endDate:
                                    dow2 = int(row[2])

                            for i in range(7 - dow1):
                                xdata.append(1)

                            for j in range(51):
                                for k in range(7):
                                    xdata.append(j + 2)

                            if year in leapYrs:
                                daysInMonth = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                                for k in range(7):
                                    xdata.append(53)

                                for l in range(1 + dow2):
                                    xdata.append(54)

                            else:
                                for l in range(1 + dow2):
                                    xdata.append(53)

                            for i in range(7):
                                if i >= dow1:
                                    ydata.append(i + 1)

                            for j in range(51):
                                for k in range(7):
                                    ydata.append(k + 1)

                            if year in leapYrs:
                                for k in range(7):
                                    ydata.append(k + 1)

                            for l in range(7):
                                if l + 1 <= dow2 + 1:
                                    ydata.append(l + 1)
                                else:
                                    break

                            hovertext = []

                            k = 0

                            for i in range(len(months)):
                                for j in range(daysInMonth[i]):
                                    s = str(j + 1)
                                    s += " "
                                    s += months[i]
                                    s += " - "
                                    s += str(numOfEmails[k])
                                    k += 1
                                    hovertext.append(s)

                            return [{'YearInput': year, 'SendersInput' : senders, 'ReceiversInput' : receivers, 'WordsInput': terms, 'MonthsInYearXAxis': xdata, 'DaysOfWeekYAxis': ydata, 'VolumeOfEmailsSentInYearZAxis': numOfEmails, 'HoverText' : hovertext, 'ErrorMessage' : 'None'}]



    # WORDCLOUDS
    # Most common terms in specified date range
    # Returns data to construct a WordCloud with JavaScript/JQuery
    class WordCloud_Most_Common_Terms_Within_Period_Of_Time(Resource):
        def get(self):
            args = request.args
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

            if startMonth==2:
                if startYear%4 ==0:
                    if startDay>29:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startDay>28:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
            elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                if startDay>30:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
            else:
                if startDay>31:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

            if endMonth==2:
                if endYear%4 ==0:
                    if startDay>29:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endDay>28:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
            elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                if endMonth>30:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
            else:
                if endDay>31:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

            termFreq = []
            terms = []
            freqs = []

            query = '''
                SELECT SUM(et.frequency), t.term FROM enron.emailterms AS et
                INNER JOIN enron.terms AS t ON t.termid = et.termid
                INNER JOIN enron.emails AS e ON e.emailid = et.emailid
                WHERE LENGTH(t.term) > 3 AND e.date BETWEEN %s AND %s
                AND t.term NOT IN ('http', 'font', 'helvetica', 'arial', 'mailto')
                GROUP BY t.term 
                ORDER BY SUM(et.frequency) DESC 
                LIMIT 50;
            '''

            cursor.execute(query, (startDate, endDate,))
            records = cursor.fetchall()
            for row in records:
                termFreq.append([row[0], row[1]])#List of lists of two items each

            random.shuffle(termFreq) # Shuffling terms, but not losing the relationship between term and frequency

            for x in termFreq:
                freqs.append(x[0])
                terms.append(x[1])

            return [{'StartDate' : str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear), 'EndDate' : str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear), 'Terms' : terms, 'Frequencies' : freqs, 'ErrorMessage' : 'None'}]

    # Most common terms between a specified set of senders and a specified set of receivers within specified date range
    # Returns data to construct a WordCloud with JavaScript/JQuery
    class WordCloud_Most_Common_Terms_Between_Senders_And_Receivers_Within_Period_Of_Time(Resource):
        def get(self):
            args = request.args
            sendersListIn = ast.literal_eval(args['senderList'])  # This translates the string into the appropriate data type
            receiversListIn = ast.literal_eval(args['receiverList'])  # This translates the string into the appropriate data type
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            if len(sendersListIn) == 0:
                return [{'ErrorMessage' : 'You did not enter any senders!'}]
            elif len(receiversListIn) == 0:
                return [{'ErrorMessage': 'You did not enter any receivers!'}]
            else:
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                sendersTupleIn = tuple(sendersListIn)
                receiversTupleIn = tuple(receiversListIn)
                senders = []
                receivers = []

                termFreq = []
                terms = []
                freqs = []

                postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s"

                # Filtering out only sender email addresses which are in the database's people table
                cursor.execute(postgres_select_query1, (sendersTupleIn,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    senders.append(row[0])

                if len(senders) == 0:
                    return [{'ErrorMessage': 'No sender email you entered was found in the corpus!'}]
                else:
                    sendersTuple = tuple(senders)

                    # Filtering out only receiver email addresses which are in the database's people table
                    cursor.execute(postgres_select_query1, (receiversTupleIn,))
                    rows1 = cursor.fetchall()
                    for row in rows1:
                        receivers.append(row[0])

                    if len(receivers) == 0:
                        return [{'ErrorMessage': 'No receiver email you entered was found in the corpus!'}]
                    else:
                        receiversTuple = tuple(receivers)

                postgres_select_query2 = '''
                    SELECT SUM(et.frequency), t.term FROM enron.emailterms AS et
                    INNER JOIN enron.terms AS t ON t.termid = et.termid
                    INNER JOIN enron.emails AS e ON e.emailid = et.emailid
                    INNER JOIN enron.people AS p ON p.personid = e.senderid
                    INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                    WHERE p.emailaddress IN %s AND r.emailaddress IN %s
                    AND LENGTH(t.term) > 3 AND e.date BETWEEN %s AND %s
                    AND t.term NOT IN ('http', 'font', 'helvetica', 'arial', 'mailto')
                    GROUP BY t.term
                    ORDER BY SUM(et.frequency) DESC
                    LIMIT 50;
                '''

                cursor.execute(postgres_select_query2, (sendersTuple, receiversTuple, startDate, endDate,))
                records = cursor.fetchall()
                for row in records:
                    termFreq.append([row[0], row[1]])

                random.shuffle(termFreq)  #Shuffling terms, but not losing the relationship between term and frequency

                for x in termFreq:
                    freqs.append(x[0])
                    terms.append(x[1])

                return [{'StartDate' : str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear), 'EndDate' : str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear), 'SendersInput' : senders , 'ReceiversInput' : receivers, 'Terms': terms, 'Frequencies': freqs, 'ErrorMessage' : 'None'}]



    # EMAIL FILTERING FUNCTIONALITY
    # Filter by date
    # Returns data pertaining to emails meeting the specified criteria
    class Get_Email_Data_Filter_By_Just_Date(Resource):
        def get(self):
            args = request.args
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

            if startMonth==2:
                if startYear%4 ==0:
                    if startDay>29:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startDay>28:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
            elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                if startDay>30:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
            else:
                if startDay>31:
                    return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

            if endMonth==2:
                if endYear%4 ==0:
                    if startDay>29:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endDay>28:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
            elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                if endMonth>30:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
            else:
                if endDay>31:
                    return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

            jsons_list = []  # To hold the returned jsons from the database

            # This query will get all the email data (except email body) as JSONs
            postgres_select_query = '''
                SELECT row_to_json(r)
                FROM(
                SELECT p.emailaddress AS sender, array_agg(r.emailaddress) AS receivers,e.date::date as date, e.subject as subject, e.numofwords AS "numberOfWords" , e.directory as "directoryAtSource", e.emailid as "emailID"
                FROM enron.emails e
                INNER JOIN enron.people p ON e.senderid = p.personid
                INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                WHERE e.date BETWEEN %s AND %s
                GROUP BY p.emailaddress, e.date, e.subject, e.numofwords, e.directory, e.emailid
                ORDER BY e.date)r;
            '''

            cursor.execute(postgres_select_query, (startDate, endDate,))
            rows = cursor.fetchall()
            for row in rows:
                jsons_list.append(row[0])

            return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear), 'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear), 'EmailData': jsons_list, 'ErrorMessage' : 'None'}]

    # Filter by people
    # Returns data pertaining to emails meeting the specified criteria
    class Get_Email_Data_Filter_By_Sender(Resource):
        def get(self):
            args = request.args
            senderEmailAddress = str(args['senderEmail'])
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            # First we check if the email address sent in is in the database's people table
            postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress = %s"

            cursor.execute(postgres_select_query1, (senderEmailAddress,))
            row = cursor.fetchone()
            if row == None:
                return [{'ErrorMessage' : 'The sender emailaddress you entered is not in the corpus!'}]
            else:
                sender = row[0]
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                jsons_list = []  # To hold the returned jsons from the database

                # This query will get all the email data (except email body) of the specified sender as JSONs
                postgres_select_query2 = '''
                    SELECT row_to_json(r)
                    FROM(
                    SELECT p.emailaddress AS sender, array_agg(r.emailaddress) as "receivers",e.date::date as date, e.subject as subject, e.numofwords AS "numberOfWords" , e.directory as "directoryAtSource", e.emailid as "emailID"
                    FROM enron.emails e
                    INNER JOIN enron.people p ON e.senderid = p.personid
                    INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                    WHERE p.emailaddress = %s AND e.date BETWEEN %s AND %s
                    GROUP BY p.emailaddress, e.date, e.subject, e.numofwords, e.directory, e.emailid
                    ORDER BY e.date)r;
                '''

                cursor.execute(postgres_select_query2, (sender, startDate, endDate,))
                rows = cursor.fetchall()
                for row in rows:
                    jsons_list.append(row[0])

                return [{'StartDate' : str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear), 'EndDate' : str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear), 'SenderEmailInput' : sender, 'EmailData' : jsons_list, 'ErrorMessage' : 'None'}]
    # Returns data pertaining to emails meeting the specified criteria
    class Get_Email_Data_Filter_By_Receiver(Resource):
        def get(self):
            args = request.args
            receiverEmailAddress = str(args['receiverEmail'])
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            # First we check if the email address sent in is in the database's people table
            postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress = %s"

            cursor.execute(postgres_select_query1, (receiverEmailAddress,))
            row = cursor.fetchone()
            if row == None:
                return [{'ErrorMessage' : 'The email you entered is not in the corpus!'}]
            else:
                receiver = row[0]
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                if startMonth==2:
                    if startYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startDay>28:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                    if startDay>30:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                else:
                    if startDay>31:
                        return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                if endMonth==2:
                    if endYear%4 ==0:
                        if startDay>29:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endDay>28:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                    if endMonth>30:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                else:
                    if endDay>31:
                        return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                jsons_list = []  # To hold the returned jsons from the database

                # This query will get all the email data (except email body) of the specified receiver as JSONs
                postgres_select_query2 = '''
                    SELECT row_to_json(r)
                    FROM(
                    SELECT p.emailaddress AS sender, array_agg(r.emailaddress) as "receivers",e.date::date as date, e.subject as subject, e.numofwords AS "numberOfWords" , e.directory as "directoryAtSource", e.emailid as "emailID"
                    FROM enron.emails e
                    INNER JOIN enron.people p ON e.senderid = p.personid
                    INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                    WHERE e.date BETWEEN %s AND %s
                    GROUP BY p.emailaddress, e.date, e.subject, e.numofwords, e.directory, e.emailid
                    HAVING %s = ANY(array_agg(r.emailaddress))
                    ORDER BY e.date)r;
                '''

                cursor.execute(postgres_select_query2, (startDate, endDate, receiver,))
                rows = cursor.fetchall()
                for row in rows:
                    jsons_list.append(row[0])

                return [{'StartDate' : str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear), 'EndDate' : str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear), 'ReceiverEmailInput' : receiver, 'EmailData' : jsons_list, 'ErrorMessage' : 'None'}]
    # Returns data pertaining to emails meeting the specified criteria
    class Get_Email_Data_Filter_By_Sender_And_Receiver(Resource):
        def get(self):
            args = request.args
            senderEmailAddress = str(args['senderEmail'])
            receiverEmailAddress = str(args['receiverEmail'])
            startYear = int(args['startYear'])
            startMonth = int(args['startMonth'])
            startDay = int(args['startDay'])
            endYear = int(args['endYear'])
            endMonth = int(args['endMonth'])
            endDay = int(args['endDay'])

            # First we check if the email addresses sent in are in the database's people table
            postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress = %s"

            cursor.execute(postgres_select_query1, (senderEmailAddress,))
            row = cursor.fetchone()
            if row == None:
                return [{'ErrorMessage' : 'The sender email you entered is not in the corpus!'}]
            else:
                sender = row[0]
                cursor.execute(postgres_select_query1, (receiverEmailAddress,))
                row = cursor.fetchone()
                if row == None:
                    return [{'ErrorMessage': 'The receiver email you entered is not in the corpus!'}]
                else:
                    receiver = row[0]
                    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                    startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                    endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

                    if startMonth==2:
                        if startYear%4 ==0:
                            if startDay>29:
                                return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                        elif startDay>28:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    elif startMonth==4 or startMonth==6 or startMonth==9 or startMonth==11:
                        if startDay>30:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]
                    else:
                        if startDay>31:
                            return [{'ErrorMessage': startDate + ' IS NOT A VALID DATE'}]

                    if endMonth==2:
                        if endYear%4 ==0:
                            if startDay>29:
                                return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                        elif endDay>28:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    elif endMonth==4 or endMonth==6 or endMonth==9 or endMonth==11:
                        if endMonth>30:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]
                    else:
                        if endDay>31:
                            return [{'ErrorMessage': endDate + ' IS NOT A VALID DATE'}]

                    jsons_list = []  # To hold the returned jsons from the database

                    # This query will get all the email data (except email body) of the specified sender and receiver as JSONs
                    postgres_select_query2 = '''
                        SELECT row_to_json(r)
                        FROM(
                        SELECT p.emailaddress AS sender, array_agg(r.emailaddress) as "receivers",e.date::date as date, e.subject as subject, e.numofwords AS "numberOfWords" , e.directory as "directoryAtSource", e.emailid as "emailID"
                        FROM enron.emails e
                        INNER JOIN enron.people p ON e.senderid = p.personid
                        INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                        WHERE e.date BETWEEN %s AND %s AND p.emailaddress = %s
                        GROUP BY p.emailaddress, e.date, e.subject, e.numofwords, e.directory, e.emailid
                        HAVING %s = ANY(array_agg(r.emailaddress))
                        ORDER BY e.date)r;
                    '''

                    cursor.execute(postgres_select_query2, (startDate, endDate, sender, receiver,))
                    rows = cursor.fetchall()
                    for row in rows:
                        jsons_list.append(row[0])

                    return [{'StartDate': str(months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear), 'EndDate': str(months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear), 'SenderEmailInput' : sender, 'ReceiverEmailInput': receiver, 'EmailData': jsons_list, 'ErrorMessage' : 'None'}]

    # Returns full details of particular email(when user clicks on an email shown through one of the functions above)
    class Get_Full_Email_Details_By_EmailID(Resource):
        def get(self):
            args = request.args
            emailIDin = int(args['emailID'])

            # First we have to ensure that the emailid received exists
            postgres_select_query1 = "SELECT emailID FROM enron.emails WHERE emailid = %s"

            cursor.execute(postgres_select_query1, (emailIDin,))
            row = cursor.fetchone()
            if row == None:
                return [{'ErrorMessage' : 'The emailid you entered is not in the corpus!'}]
            else:
                emailID = row[0]

                # This query will get all the email data (INCLUDING the email body) as JSONs
                postgres_select_query2 = '''
                    SELECT row_to_json(r)
                    FROM(
                    SELECT p.emailaddress AS sender, array_agg(r.emailaddress) as "receivers",e.date::date as date, e.subject as subject, eft.emailtext ,e.numofwords AS "numberOfWords" , e.directory as "directoryAtSource", e.emailid as "emailID"
                    FROM enron.emails e
                    INNER JOIN enron.people p ON e.senderid = p.personid
                    INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid
                    INNER JOIN enron.emailfulltext eft ON e.emailid = eft.emailid
                    WHERE e.emailid = %s
                    GROUP BY p.emailaddress, e.date, e.subject, eft.emailtext ,e.numofwords, e.directory, e.emailid
                    ORDER BY e.date
                    )r;
                '''

                cursor.execute(postgres_select_query2, (emailID,))
                row = cursor.fetchone()
                return [{'EmailIDEntered' : emailID, 'EmailDetails' : row[0], 'ErrorMessage' : 'None'}]



    # VIEW SENDERS AND RECEIVERS FUNCTIONALITY
    # View Top X Senders(According to no. of emails sent)
    class View_Top_X_Senders(Resource):
        def get(self):
            args = request.args
            amount = int(args['amount'])

            if amount < 1 or amount > 100:
                return  [{'ErrorMessage' : 'Enter a number between 1 and 100'}]
            else:
                jsons_list = []

                postgres_select_query1 = '''
                    SELECT row_to_json(r)
                    FROM(
                    SELECT emailaddress, sentemails, receivedemails FROM enron.people WHERE sentemails != 0 ORDER BY sentemails DESC LIMIT %s)r;
                '''

                cursor.execute(postgres_select_query1, (amount,))
                rows = cursor.fetchall()
                for row in rows:
                    jsons_list.append(row[0])

                return [{'AmountInput' : amount, 'Senders': jsons_list, 'ErrorMessage': 'None'}]
    # View Top X Receivers(According to no. of emails received)
    class View_Top_X_Receivers(Resource):
        def get(self):
            args = request.args
            amount = int(args['amount'])

            if amount < 1 or amount > 100:
                return  [{'ErrorMessage' : 'Enter a number between 1 and 100'}]
            else:
                jsons_list = []

                postgres_select_query1 = '''
                    SELECT row_to_json(r)
                    FROM(
                    SELECT emailaddress, sentemails, receivedemails FROM enron.people WHERE receivedemails != 0 ORDER BY receivedemails DESC LIMIT %s)r;
                '''

                cursor.execute(postgres_select_query1, (amount,))
                rows = cursor.fetchall()
                for row in rows:
                    jsons_list.append(row[0])

                return [{'AmountInput' : amount, 'Receivers': jsons_list, 'ErrorMessage': 'None'}]

    # Search for people (by email address)
    class Search_For_Emails(Resource):
        def get(self):
            args = request.args
            searchString = str(args['searchString'])

            searchString2 = '%'+searchString+'%' #To look for the string as a substring of another string rather than only looking for it exactly
            jsons_list = []

            postgres_select_query1 = '''
                SELECT row_to_json(r)
                FROM(
                SELECT emailaddress, sentemails, receivedemails FROM enron.people WHERE emailaddress LIKE %s ORDER BY sentemails DESC, receivedemails DESC LIMIT 80)r;
            '''

            cursor.execute(postgres_select_query1, (searchString2,))
            rows = cursor.fetchall()
            for row in rows:
                jsons_list.append(row[0])

            return [{'SearchString' : searchString, 'Emails' : jsons_list, 'ErrorMessage': 'None'}]



    api.add_resource(Most_Popular_Words_In_Particular_Year_Grouped_By_Month, '/MostPopularWordsInParticularYearGroupedByMonth')
    api.add_resource(Most_Popular_Words_In_Particular_Month_Grouped_By_Day, '/MostPopularWordsInParticularMonthGroupedByDay')
    api.add_resource(Most_Popular_Words_In_Particular_Day, '/MostPopularWordsInParticularDay')
    api.add_resource(Most_Popular_Years_Words_Were_Mentioned, '/MostPopularYearsWordsWereMentioned')
    api.add_resource(Most_Popular_Months_Words_Were_Mentioned, '/MostPopularMonthsWordsWereMentioned')
    api.add_resource(Most_Popular_Days_Words_Were_Mentioned, '/MostPopularDaysWordsWereMentioned')
    api.add_resource(Word_Mentions_In_Year_Grouped_By_Month, '/WordMentionsInYearGroupedByMonth')
    api.add_resource(Word_Mentions_In_Month_Grouped_By_Day, '/WordMentionsInMonthGroupedByDay')
    api.add_resource(Word_Mentions_In_Particular_Day, '/WordMentionsInParticularDay')
    api.add_resource(three_term_frequency_comparative_analysis_over_single_year, '/ThreeTermFrequencyComparativeAnalysisOverSingleYear')
    api.add_resource(three_term_frequency_comparative_analysis_over_single_month, '/ThreeTermFrequencyComparativeAnalysisOverSingleMonth')
    api.add_resource(three_year_volume_of_emails_sent_with_particular_terms_comparative_analysis, '/ThreeYearVolumeOfEmailsSentWithParticularTermsComparativeAnalysis')
    api.add_resource(three_month_volume_of_emails_sent_with_particular_terms_comparative_analysis, '/ThreeMonthVolumeOfEmailsSentWithParticularTermsComparativeAnalysis')

    api.add_resource(Most_Frequent_Senders_Within_Specified_Date_Range, '/MostFrequentSendersWithinSpecifiedDateRange')
    api.add_resource(Most_Frequent_Receivers_Within_Specified_Date_Range, '/MostFrequentReceiversWithinSpecifiedDateRange')
    api.add_resource(How_Many_Emails_Senders_Sent_Within_Time_Period, '/HowManyEmailsSendersSentWithinTimePeriod')
    api.add_resource(How_Many_Emails_Receivers_Received_Within_Time_Period, '/HowManyEmailsReceiversReceivedWithinTimePeriod')

    api.add_resource(Most_Frequent_Senders_Of_Words_Within_Time_Period, '/MostFrequentSendersOfWordsWithinTimePeriod')
    api.add_resource(Most_Frequent_Receivers_Of_Words_Within_Time_Period, '/MostFrequentReceiversOfWordsWithinTimePeriod')
    api.add_resource(Senders_Words_Frequency_Within_Time_Period, '/SendersWordsFrequencyWithinTimePeriod')
    api.add_resource(Receivers_Words_Frequency_Within_Time_Period, '/ReceiversWordsFrequencyWithinTimePeriod')
    api.add_resource(Senders_Most_Popular_Words_Within_Time_Period, '/SendersMostPopularWordsWithinTimePeriod')
    api.add_resource(Receivers_Most_Popular_Words_Within_Time_Period, '/ReceiversMostPopularWordsWithinTimePeriod')

    api.add_resource(correspondence_Heatmap_overall, '/CorrespondenceHeatmapOverall')
    api.add_resource(correspondence_Heatmap_Within_Time_Period, '/CorrespondenceHeatmapWithinTimePeriod')
    api.add_resource(correspondence_Heatmap_With_Particular_Terms, '/CorrespondenceHeatmapWithParticularTerms')
    api.add_resource(correspondence_Heatmap_With_Particular_Terms_Within_Time_Period, '/CorrespondenceHeatmapWithParticularTermsWithinTimePeriod')

    api.add_resource(calendar_heatmap_for_year, '/CalendarHeatmapForYear')
    api.add_resource(calendar_heatmap_for_year_with_particular_senders, '/CalendarHeatmapForYearWithParticularSenders')
    api.add_resource(calendar_heatmap_for_year_with_particular_receivers, '/CalendarHeatmapForYearWithParticularReceivers')
    api.add_resource(calendar_heatmap_for_year_with_particular_terms, '/CalendarHeatmapForYearWithParticularTerms')
    api.add_resource(calendar_heatmap_for_year_with_particular_senders_receivers_terms, '/CalendarHeatmapForYearWithParticularSendersReceiversTerms')

    api.add_resource(WordCloud_Most_Common_Terms_Within_Period_Of_Time, '/WordCloudMostCommonTermsWithinPeriodOfTime')
    api.add_resource(WordCloud_Most_Common_Terms_Between_Senders_And_Receivers_Within_Period_Of_Time, '/WordCloudMostCommonTermsBetweenSendersAndReceiversWithinPeriodOfTime')

    api.add_resource(Get_Email_Data_Filter_By_Just_Date, '/GetEmailDataFilterByJustDate')
    api.add_resource(Get_Email_Data_Filter_By_Sender, '/GetEmailDataFilterBySender')
    api.add_resource(Get_Email_Data_Filter_By_Receiver, '/GetEmailDataFilterByReceiver')
    api.add_resource(Get_Email_Data_Filter_By_Sender_And_Receiver, '/GetEmailDataFilterBySenderAndReceiver')
    api.add_resource(Get_Full_Email_Details_By_EmailID, '/GetFullEmailDetailsByEmailID')

    api.add_resource(View_Top_X_Senders, '/ViewTopXSenders')
    api.add_resource(View_Top_X_Receivers, '/ViewTopXReceivers')
    api.add_resource(Search_For_Emails, '/SearchForEmails')


    if __name__ == '__main__':
        app.run(debug=True)

except (Exception, psycopg2.Error) as error:
    print("ERROR IN DB OPERATION:", error)
finally:
# closing database connection.
    if (connection):
        # Close cursor object and database connection
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
