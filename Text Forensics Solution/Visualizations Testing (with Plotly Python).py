import psycopg2
from datetime import datetime
import random
import json
import plotly
import plotly.plotly as py
import plotly.graph_objs as go
plotly.tools.set_credentials_file(username='nate99', api_key='6G62uXAtK4KQkVw8HhYv')

try:

    connection = psycopg2.connect(user="postgres", password="1234", host="127.0.0.1", port="5432", database="EnronDB")
    cursor = connection.cursor()  # creating a cursor object using the connection object

    #START. OF. FUNCTIONS.

    #FUNCTIONS COMBINING WORDS AND DATES
    #Most common words within specified time period
    # HeatMap
    def Most_Popular_Words_In_Particular_Year_Grouped_By_Month(year):

        # These will be passed to the plotly heatmap
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
            print('No emails were sent on this date!')
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
                cursor.execute(postgres_select_query2, (wordsTuple, year, monthNumber, wordsTuple,))
                rows = cursor.fetchall();
                for row in rows:
                    frequencies_per_month_list.append(row[0])
                frequencies.append(frequencies_per_month_list)

            trace = go.Heatmap(z=frequencies,
                               x=words,
                               y=months,
                               xgap=5,
                               ygap=5,
                               colorscale='Reds')
            data = [trace]
            layout = go.Layout(
                xaxis=dict(
                    title='TERMS',
                    autorange=True,
                    showgrid=True,
                    zeroline=True,
                    showline=True,
                    automargin=True,
                    # fixedrange = True,
                    showticklabels=True,
                    tickangle=45
                ),
                yaxis=dict(
                    title='MONTHS IN ' + str(year),
                    autorange=True,
                    showgrid=True,
                    zeroline=True,
                    showline=False,
                    automargin=True,
                    # fixedrange=True,
                    showticklabels=True,
                )
            )
            fig = go.Figure(data=data, layout=layout)
            py.plot(fig, filename='most_popular_words_per_month_by_year')
    # HeatMap
    def Most_Popular_Words_In_Particular_Month_Grouped_By_Day(year, month):

        months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']

        # These will be passed to the plotly heatmap
        days = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
                29, 30, 31]
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
            print('No emails were sent on this date!')
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
                cursor.execute(postgres_select_query2, (wordsTuple, year, month, date, wordsTuple,))
                rows = cursor.fetchall();
                for row in rows:
                    frequencies_per_day_list.append(row[0])
                frequencies.append(frequencies_per_day_list)

            trace = go.Heatmap(z=frequencies,
                               x=words,
                               y=days,
                               xgap=5,
                               ygap=5,
                               colorscale='Reds')
            data = [trace]
            layout = go.Layout(
                xaxis=dict(
                    title='TERMS',
                    autorange=True,
                    showgrid=True,
                    zeroline=True,
                    showline=True,
                    automargin=True,
                    # fixedrange = True,
                    showticklabels=True,
                    tickangle=45
                ),
                yaxis=dict(
                    title=str('DAYS IN ' + months[month - 1]) + ' ' + str(year),
                    autorange=True,
                    showgrid=True,
                    zeroline=True,
                    showline=False,
                    automargin=True,
                    # fixedrange=True,
                    showticklabels=True,
                )
            )
            fig = go.Figure(data=data, layout=layout)
            py.plot(fig, filename='most_popular_words_per_day_by_month')
    # BarChart
    def Most_Popular_Words_In_Particular_Day(year, month, day):

        months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']

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
            print('No emails were sent on this day!')
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
            rows = cursor.fetchall();
            for row in rows:
                frequencies.append(row[0])
            print(frequencies)
            print(len(frequencies))

            data = [go.Bar(
                x=words,
                y=frequencies,
                marker=dict(
                    color='rgb(209, 33, 2)')
            )]
            layout = go.Layout(
                xaxis=dict(
                    title='MOST POPULAR TERMS ON ' + str(months[month - 1]) + ' ' + str(day) + ' ' + str(year),
                    autorange=True,
                    showgrid=True,
                    zeroline=True,
                    showline=True,
                    automargin=True,
                    # fixedrange = True,
                    showticklabels=True,
                    tickangle=45
                ),
                yaxis=dict(
                    title='FREQUENCY',
                    autorange=True,
                    showgrid=True,
                    zeroline=True,
                    showline=False,
                    automargin=True,
                    # fixedrange=True,
                    showticklabels=True,
                )
            )
            fig = go.Figure(data=data, layout=layout)
            py.plot(fig, filename='Most_Popular_Words_In_Day_Bar_Chart')

    #When were specified words mentioned the most? (most popular years, months, or days)
    #HeatMap
    def Most_Popular_Years_Words_Were_Mentioned(wordsList):
            if len(wordsList) == 0:
                print('You did not enter any word!')
            else:
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
                    print('The word/s you entered are not in the corpus')
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

                    trace = go.Heatmap(z=frequencies,
                                       x=years,
                                       y=words,
                                       xgap=5,
                                       ygap=5,
                                       colorscale='Reds')

                    data = [trace]
                    layout = go.Layout(
                        xaxis=dict(
                            title='Years',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=True,
                            automargin=True,
                            # fixedrange = True,
                            showticklabels=True,
                            tickangle=45
                        ),
                        yaxis=dict(
                            title='Words',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=False,
                            automargin=True,
                            # fixedrange=True,
                            showticklabels=True
                        )
                    )
                    fig = go.Figure(data=data, layout=layout)
                    py.plot(fig, filename='Most_Popular_Years_Words_Were_Mentioned')
    #HeatMap
    def Most_Popular_Months_Words_Were_Mentioned(wordsList):
            if len(wordsList) == 0:
                print('You did not enter any word!')
            else:
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
                    print('The word/s you entered are not in the corpus')
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

                    trace = go.Heatmap(z=frequencies,
                                       x=dates,
                                       y=words,
                                       xgap=5,
                                       ygap=5,
                                       colorscale='Reds')

                    data = [trace]
                    layout = go.Layout(
                        xaxis=dict(
                            title='Months and Years',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=True,
                            automargin=True,
                            # fixedrange = True,
                            showticklabels=True,
                            tickangle=45
                        ),
                        yaxis=dict(
                            title='Words',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=False,
                            automargin=True,
                            # fixedrange=True,
                            showticklabels=True,
                        )
                    )
                    fig = go.Figure(data=data, layout=layout)
                    py.plot(fig, filename='Months_Words_Were_Mentioned_Heatmap')
    #HeatMap
    def Most_Popular_Days_Words_Were_Mentioned(wordsList):
            if len(wordsList) == 0:
                print('You did not enter any word!')
            else:
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
                    print('The word/s you entered are not in the corpus')
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

                    trace = go.Heatmap(z=frequencies,
                                       x=dates,
                                       y=words,
                                       xgap=5,
                                       ygap=5,
                                       colorscale='Reds')

                    data = [trace]
                    layout = go.Layout(
                        xaxis=dict(
                            title='Days and Months and Years',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=True,
                            automargin=True,
                            # fixedrange = True,
                            showticklabels=True,
                            tickangle=45
                        ),
                        yaxis=dict(
                            title='Words',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=False,
                            automargin=True,
                            # fixedrange=True,
                            showticklabels=True,
                        )
                    )
                    fig = go.Figure(data=data, layout=layout)
                    py.plot(fig, filename='Days_Words_Were_Mentioned_Heatmap')

    #How many times were specified terms mentioned within specified time period?
    #HeatMap
    def Word_Mentions_In_Year_Grouped_By_Month(wordsList, year):
        if len(wordsList) == 0:
            print('You did not enter any word!')
        else:
            wordsTuple = tuple(wordsList)
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']  # On x-axis
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
                print('The word/s you entered are not in the corpus')
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

                trace = go.Heatmap(x=months,
                                   y=words,
                                   z=frequency,
                                   xgap=5,
                                   ygap=5,
                                   colorscale='Reds')

                data = [trace]
                layout = go.Layout(
                    xaxis=dict(
                        title='MONTHS IN THE YEAR ' + str(year),
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=True,
                        automargin=True,
                        # fixedrange = True,
                        showticklabels=True,
                        tickangle=45
                    ),
                    yaxis=dict(
                        title='TERMS',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=False,
                        automargin=True,
                        # fixedrange=True,
                        showticklabels=True,
                    )
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='Word_Mentions_In_Year_Grouped_By_Month_Heatmap')
    #HeatMap
    def Word_Mentions_In_Month_Grouped_By_Day(wordsList, year, month):
        if len(wordsList) == 0:
            print('You did not enter any word!')
        else:
            wordsTuple = tuple(wordsList)
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
            daysInMonth = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
                           26, 27, 28, 29, 30, 31]
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
                print('The word/s you entered are not in the corpus')
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

                trace = go.Heatmap(x=daysInMonth,
                                   y=words,
                                   z=frequency,
                                   xgap=5,
                                   ygap=5,
                                   colorscale='Reds')

                data = [trace]
                layout = go.Layout(
                    xaxis=dict(
                        title='DAYS IN ' + str(months[month - 1]) + ' ' + str(year),
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=True,
                        automargin=True,
                        # fixedrange = True,
                        showticklabels=True,
                        tickangle=45
                    ),
                    yaxis=dict(
                        title='TERMS',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=False,
                        automargin=True,
                        # fixedrange=True,
                        showticklabels=True,
                    )
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='Word_Mentions_In_Month_Grouped_By_Day')
    #BarChart
    def Word_Mentions_In_Particular_Day(wordsList, year, month, day):
        if len(wordsList) == 0:
            print('You did not enter any word!')
        else:
            wordsTuple = tuple(wordsList)
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
            words = []  # On x-axis
            frequency = []  # On y-axis

            postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

            # Filtering only words which are in the database terms table
            cursor.execute(postgres_select_query1, (wordsTuple,))
            rows1 = cursor.fetchall()
            for row in rows1:
                words.append(row[0])
            if len(words) == 0:
                print('The word/s you entered are not in the corpus')
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

                data = [go.Bar(
                    x=words,
                    y=frequency,
                    marker=dict(
                        color='rgb(209, 33, 2)')
                )]
                layout = go.Layout(
                    xaxis=dict(
                        title='TERMS',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=True,
                        automargin=True,
                        # fixedrange = True,
                        showticklabels=True,
                        tickangle=45
                    ),
                    yaxis=dict(
                        title='FREQUENCIES ON ' + str(months[month - 1]) + ' ' + str(day) + ' ' + str(year),
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=False,
                        automargin=True,
                        # fixedrange=True,
                        showticklabels=True,
                    )
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='Word_Mentions_In_Particular_Day_BarGraph')

    #Comparative Analysis Functions
    #Frequency of three terms over specified time period
    #LinePlot with 3 Lines
    def three_term_frequency_comparative_analysis_over_single_year(three_words_list, year):
        if len(three_words_list) != 3:
            print('You have to enter 3 terms!')
        else:
            termsTuple = tuple(three_words_list)
            termsList = []
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']  # This will be on the x-axis
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
                print('A word/s from the three words you entered are not in the corpus!')
            else:
                termsList.sort()  # Sorting the terms alphabetically
                print(termsList)
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

                trace0 = go.Scatter(
                    x=months,
                    y=frequencies_word_1,
                    mode='lines+markers',
                    name=termsList[0]
                )
                trace1 = go.Scatter(
                    x=months,
                    y=frequencies_word_2,
                    mode='lines+markers',
                    name=termsList[1]
                )
                trace2 = go.Scatter(
                    x=months,
                    y=frequencies_word_3,
                    mode='lines+markers',
                    name=termsList[2]
                )

                data = [trace0, trace1, trace2]
                layout = go.Layout(
                    width=1400,
                    height=600,
                    xaxis=dict(
                        title='MONTHS IN THE YEAR ' + str(year),
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=True,
                        automargin=True,
                        fixedrange=True,
                        showticklabels=True,
                        tickangle=45
                    ),
                    yaxis=dict(
                        title='FREQUENCY OF TERMS',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=False,
                        automargin=True,
                        fixedrange=True,
                        showticklabels=True,
                    )
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='three_term_comparative_line_plot_over_single_year')
    #LinePlot with 3 Lines
    def three_term_frequency_comparative_analysis_over_single_month(three_words_list, year, month):
        if len(three_words_list) != 3:
            print('You have to enter 3 terms!')
        else:
            termsTuple = tuple(three_words_list)
            termsList = []
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
            daysInMonth = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
                           26, 27, 28, 29, 30, 31]  # This will be on the x-axis
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
                print('A word/s from the three words you entered are not in the corpus!')
            else:
                termsList.sort()  # Sorting the terms alphabetically
                print(termsList)
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

                trace0 = go.Scatter(
                    x=daysInMonth,
                    y=frequencies_word_1,
                    mode='lines+markers',
                    name=termsList[0]
                )
                trace1 = go.Scatter(
                    x=daysInMonth,
                    y=frequencies_word_2,
                    mode='lines+markers',
                    name=termsList[1]
                )
                trace2 = go.Scatter(
                    x=daysInMonth,
                    y=frequencies_word_3,
                    mode='lines+markers',
                    name=termsList[2]
                )

                data = [trace0, trace1, trace2]
                layout = go.Layout(
                    width=1400,
                    height=600,
                    xaxis=dict(
                        title='DAYS IN THE MONTH ' + str(months[month - 1]) + ' ' + str(year),
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=True,
                        automargin=True,
                        fixedrange=True,
                        showticklabels=True,
                        tickangle=45
                    ),
                    yaxis=dict(
                        title='FREQUENCY OF TERMS',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=False,
                        automargin=True,
                        fixedrange=True,
                        showticklabels=True,
                    )
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='three_term_comparative_line_plot_over_single_month')

    #Comparative Analysis Functions
    #Volume of emails sent over three specified years or months
    #LinePlot with 3 Lines
    def three_year_volume_of_emails_sent_with_particular_terms_comparative_analysis(year1, year2, year3, termsListIn):
        if year1 < 1900 or year1 > 2019 or year2 < 1900 or year2 > 2019 or year3 < 1900 or year3 > 2019:
            print('One or more of the years you entered are invalid!')
        elif len(termsListIn) == 0:
            print('You did not enter any terms!')
        else:
            termsTupleIn = tuple(termsListIn)
            terms = []
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']  # This will be on the x-axis
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
                print('No word you entered is in the corpus!')
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

                trace0 = go.Scatter(
                    x=months,
                    y=volume_year1,
                    mode='lines+markers',
                    name=year1
                )
                trace1 = go.Scatter(
                    x=months,
                    y=volume_year2,
                    mode='lines+markers',
                    name=year2
                )
                trace2 = go.Scatter(
                    x=months,
                    y=volume_year3,
                    mode='lines+markers',
                    name=year3
                )

                data = [trace0, trace1, trace2]
                layout = go.Layout(
                    width=1400,
                    height=600,
                    title='VOLUME OF EMAILS SENT CONTAINING ONE OR MORE TERMS FROM \n' + str(terms),
                    xaxis=dict(
                        title='MONTHS IN THE YEAR',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=True,
                        automargin=True,
                        fixedrange=True,
                        showticklabels=True,
                        tickangle=45
                    ),
                    yaxis=dict(
                        title='VOLUME OF EMAILS SENT',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=False,
                        automargin=True,
                        fixedrange=True,
                        showticklabels=True,
                    )
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='three_year_volume_of_emails_sent_with_particular_terms_comparative_analysis')
    #LinePlot with 3 Lines
    def three_month_volume_of_emails_sent_with_particular_terms_comparative_analysis(year1, month1, year2, month2, year3, month3, termsListIn):
        if year1 < 1900 or year1 > 2019 or year2 < 1900 or year2 > 2019 or year3 < 1900 or year3 > 2019:
            print('One or more of the years you entered are invalid!')
        elif month1 < 1 or month1 > 12 or month2 < 1 or month2 > 12 or month3 < 1 or month3 > 12:
            print('One or more of the months you entered are invalid!')
        elif len(termsListIn) == 0:
            print('You did not enter any terms!')
        else:
            termsTupleIn = tuple(termsListIn)
            terms = []
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']

            daysInMonth = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
                           26, 27, 28, 29, 30, 31]  # This will be on the x-axis
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
                print('No word you entered is in the corpus!')
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

                trace0 = go.Scatter(
                    x=daysInMonth,
                    y=volume_month1,
                    mode='lines+markers',
                    name=str(months[month1 - 1]) + ' ' + str(year1)
                )
                trace1 = go.Scatter(
                    x=daysInMonth,
                    y=volume_month2,
                    mode='lines+markers',
                    name=str(months[month2 - 1]) + ' ' + str(year2)
                )
                trace2 = go.Scatter(
                    x=daysInMonth,
                    y=volume_month3,
                    mode='lines+markers',
                    name=str(months[month3 - 1]) + ' ' + str(year3)
                )

                data = [trace0, trace1, trace2]
                layout = go.Layout(
                    width=1400,
                    height=600,
                    title='VOLUME OF EMAILS SENT CONTAINING ONE OR MORE TERMS FROM \n' + str(terms),
                    xaxis=dict(
                        title='DAYS IN THE MONTH',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=True,
                        automargin=True,
                        fixedrange=True,
                        showticklabels=True,
                        tickangle=45
                    ),
                    yaxis=dict(
                        title='VOLUME OF EMAILS SENT',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=False,
                        automargin=True,
                        fixedrange=True,
                        showticklabels=True,
                    )
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='')



    #FUNCTIONS COMBINING PEOPLE AND TIME
    #Who sent most emails or received most emails in a specified date range?
    #BarChart
    def Most_Frequent_Senders_Within_Specified_Date_Range(startYear, startMonth, startDay, endYear, endMonth, endDay):
        startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
        endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

        months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
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
            print('No one sent an email within the specified date range')
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

            data = [go.Bar(
                x=senders,
                y=frequency,
                marker=dict(
                    color='rgb(209, 33, 2)')
            )]
            layout = go.Layout(
                xaxis=dict(
                    title='MOST FREQUENT SENDERS IN THE DATE RANGE ' + str(
                        months[startMonth - 1]) + ' ' + str(
                        startDay) + ' ' + str(startYear) + ' - ' + str(months[endMonth - 1]) + ' ' + str(
                        endDay) + ' ' + str(endYear),
                    autorange=True,
                    showgrid=True,
                    zeroline=True,
                    showline=True,
                    automargin=True,
                    # fixedrange = True,
                    showticklabels=True,
                    tickangle=45
                ),
                yaxis=dict(
                    title='FREQUENCY OF EMAILS SENT',
                    autorange=True,
                    showgrid=True,
                    zeroline=True,
                    showline=False,
                    automargin=True,
                    # fixedrange=True,
                    showticklabels=True,
                )
            )
            fig = go.Figure(data=data, layout=layout)
            py.plot(fig, filename='Most_Frequent_Senders_Within_Specified_Date_Range')
    #BarChart
    def Most_Frequent_Receivers_Within_Specified_Date_Range(startYear, startMonth, startDay, endYear, endMonth, endDay):
        startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
        endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

        months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
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
            print('No one received an email within the specified date range')
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

            data = [go.Bar(
                x=receivers,
                y=frequency,
                marker=dict(
                    color='rgb(209, 33, 2)')
            )]
            layout = go.Layout(
                xaxis=dict(
                    title='MOST FREQUENT RECEIVERS IN THE DATE RANGE ' + str(
                        months[startMonth - 1]) + ' ' + str(
                        startDay) + ' ' + str(startYear) + ' - ' + str(months[endMonth - 1]) + ' ' + str(
                        endDay) + ' ' + str(endYear),
                    autorange=True,
                    showgrid=True,
                    zeroline=True,
                    showline=True,
                    automargin=True,
                    # fixedrange = True,
                    showticklabels=True,
                    tickangle=45
                ),
                yaxis=dict(
                    title='FREQUENCY OF EMAILS SENT',
                    autorange=True,
                    showgrid=True,
                    zeroline=True,
                    showline=False,
                    automargin=True,
                    # fixedrange=True,
                    showticklabels=True,
                )
            )
            fig = go.Figure(data=data, layout=layout)
            py.plot(fig, filename='Most_Frequent_Receivers_Within_Specified_Date_Range')

    #Volume of emails sent by specified senders or received by specified receivers within specified date range
    #BarChart
    def How_Many_Emails_Senders_Sent_Within_Time_Period(sendersInList, startYear, startMonth, startDay, endYear, endMonth, endDay):
        if sendersInList == 0:
            print('You did not enter any sender emails!')
        else:
            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
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
                print('No sender emails you entered were found in the corpus!')
            else:
                senders.sort()  # Sorting sender emails alphabetically
                sendersTuple = tuple(senders)
                print(sendersTuple)

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

                data = [go.Bar(
                    x=senders,
                    y=frequency,
                    marker=dict(
                        color='rgb(209, 33, 2)')
                )]
                layout = go.Layout(
                    xaxis=dict(
                        title='SPECIFIED SENDERS (DATE RANGE: ' + str(
                            months[startMonth - 1]) + ' ' + str(
                            startDay) + ' ' + str(startYear) + ' - ' + str(months[endMonth - 1]) + ' ' + str(
                            endDay) + ' ' + str(endYear) + ')',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=True,
                        automargin=True,
                        # fixedrange = True,
                        showticklabels=True,
                        tickangle=45
                    ),
                    yaxis=dict(
                        title='FREQUENCY OF EMAILS SENT',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=False,
                        automargin=True,
                        # fixedrange=True,
                        showticklabels=True,
                    )
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='How_Many_Emails_Senders_Sent_Within_Time_Period')
    #BarChart
    def How_Many_Emails_Receivers_Received_Within_Time_Period(receiversInList, startYear, startMonth, startDay, endYear, endMonth, endDay):
        if receiversInList == 0:
            print('You did not enter any receiver emails!')
        else:
            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)

            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
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
                print('No receiver emails you entered were found in the corpus!')
            else:
                receivers.sort()  # Sorting receiver emails alphabetically
                receiversTuple = tuple(receivers)
                print(receiversTuple)

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

                data = [go.Bar(
                    x=receivers,
                    y=frequency,
                    marker=dict(
                        color='rgb(209, 33, 2)')
                )]
                layout = go.Layout(
                    xaxis=dict(
                        title='SPECIFIED RECEIVERS (DATE RANGE: ' + str(
                            months[startMonth - 1]) + ' ' + str(
                            startDay) + ' ' + str(startYear) + ' - ' + str(months[endMonth - 1]) + ' ' + str(
                            endDay) + ' ' + str(endYear) + ')',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=True,
                        automargin=True,
                        # fixedrange = True,
                        showticklabels=True,
                        tickangle=45
                    ),
                    yaxis=dict(
                        title='FREQUENCY OF EMAILS RECEIVED',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=False,
                        automargin=True,
                        # fixedrange=True,
                        showticklabels=True,
                    )
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='How_Many_Emails_Receivers_Received_Within_Time_Period')



    #FUNCTIONS COMBINING PEOPLE, WORDS AND TIME
    #Who sent or received specified words the most within a specified date range?
    #HeatMap
    def Most_Frequent_Senders_Of_Words_Within_Time_Period(wordsInList, startYear, startMonth, startDay, endYear, endMonth, endDay):
        if len(wordsInList) == 0:
            print('You did not enter any words!')
        else:
            wordsInTuple = tuple(wordsInList)
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
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
                print('The word/s you entered were not found in the corpus')
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
                    print('No senders have been found that sent the specified word/s within the specified time period!')
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

                    trace = go.Heatmap(x=words,
                                       y=senders,
                                       z=frequencies,
                                       xgap=5,
                                       ygap=5,
                                       colorscale='Reds')
                    data = [trace]
                    layout = go.Layout(
                        xaxis=dict(
                            title='SPECIFIED TERMS',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=True,
                            automargin=True,
                            # fixedrange = True,
                            showticklabels=True,
                            tickangle=45
                        ),
                        yaxis=dict(
                            title='MOST FREQUENT SENDERS OF WORDS IN TIME PERIOD ' + str(
                                months[startMonth - 1]) + ' ' + str(
                                startDay) + ' ' + str(startYear) + ' - ' + str(months[endMonth - 1]) + ' ' + str(
                                endDay) + ' ' + str(endYear),
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=False,
                            automargin=True,
                            # fixedrange=True,
                            showticklabels=True,
                        )
                    )
                    fig = go.Figure(data=data, layout=layout)
                    py.plot(fig, filename='Most_Frequent_Senders_Of_Words_Within_Time_Period')
    #HeatMap
    def Most_Frequent_Receivers_Of_Words_Within_Time_Period(wordsInList, startYear, startMonth, startDay, endYear, endMonth, endDay):
        if len(wordsInList) == 0:
            print('You did not enter any words!')
        else:
            wordsInTuple = tuple(wordsInList)
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
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
                print('The word/s you entered were not found in the corpus')
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
                    print(
                        'No receivers have been found that received the specified word/s within the specified time period!')
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
                        cursor.execute(postgres_select_query3, (wordsTuple, receiver, wordsTuple, startDate, endDate))
                        rows3 = cursor.fetchall()
                        for row in rows3:
                            words_Frequencies_per_Receiver.append(row[1])
                        frequencies.append(words_Frequencies_per_Receiver)

                    trace = go.Heatmap(x=words,
                                       y=receivers,
                                       z=frequencies,
                                       xgap=5,
                                       ygap=5,
                                       colorscale='Reds')
                    data = [trace]
                    layout = go.Layout(
                        xaxis=dict(
                            title='SPECIFIED TERMS',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=True,
                            automargin=True,
                            # fixedrange = True,
                            showticklabels=True,
                            tickangle=45
                        ),
                        yaxis=dict(
                            title='MOST FREQUENT RECEIVERS OF WORDS IN TIME PERIOD ' + str(
                                months[startMonth - 1]) + ' ' + str(
                                startDay) + ' ' + str(startYear) + ' - ' + str(months[endMonth - 1]) + ' ' + str(
                                endDay) + ' ' + str(endYear),
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=False,
                            automargin=True,
                            # fixedrange=True,
                            showticklabels=True,
                        )
                    )
                    fig = go.Figure(data=data, layout=layout)
                    py.plot(fig, filename='Most_Frequent_Receivers_Of_Words_Within_Time_Period')

    #How many of a specified set of words did specified senders send or specified receivers receive within a specified date range?
    #HeatMap
    def Senders_Words_Frequency_Within_Time_Period(senderEmailList, wordsInList, startYear, startMonth, startDay, endYear, endMonth, endDay):
        if len(senderEmailList) == 0:
            print('You did not enter any sender emails!')
        elif len(wordsInList) == 0:
            print('You did not enter any words!')
        else:
            senderEmailsTuple = tuple(senderEmailList)
            wordsInTuple = tuple(wordsInList)
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
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
                print('The sender email/s you entered were not found in the corpus')
            else:
                senders.sort()  # Sorting sender emails alphabetically

                # Filtering only terms which are in the database terms table
                cursor.execute(postgres_select_query2, (wordsInTuple,))
                rows2 = cursor.fetchall()
                for row in rows2:
                    words.append(row[0])
                if len(words) == 0:
                    print('The word/s you entered were not found in the corpus')
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

                    trace = go.Heatmap(x=words,
                                       y=senders,
                                       z=frequencies,
                                       xgap=5,
                                       ygap=5,
                                       colorscale='Reds')
                    data = [trace]
                    layout = go.Layout(
                        xaxis=dict(
                            title='SPECIFIED TERMS (DATE RANGE BETWEEN' + str(months[startMonth - 1]) + ' ' + str(
                                startDay) + ' ' + str(startYear) + ' AND ' + str(months[endMonth - 1]) + ' ' + str(
                                endDay) + ' ' + str(endYear) + ')',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=True,
                            automargin=True,
                            # fixedrange = True,
                            showticklabels=True,
                            tickangle=45
                        ),
                        yaxis=dict(
                            title='SPECIFIED SENDERS',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=False,
                            automargin=True,
                            # fixedrange=True,
                            showticklabels=True,
                        )
                    )
                    fig = go.Figure(data=data, layout=layout)
                    py.plot(fig, filename='Senders_Words_Frequency_Within_Time_Period')
    #HeatMap
    def Receivers_Words_Frequency_Within_Time_Period(receiversEmailList, wordsInList, startYear, startMonth, startDay, endYear, endMonth, endDay):
        if len(receiversEmailList) == 0:
            print('You did not enter any receiver emails!')
        elif len(wordsInList) == 0:
            print('You did not enter any words!')
        else:
            receiversEmailsTuple = tuple(receiversEmailList)
            wordsInTuple = tuple(wordsInList)
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
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
                print('The receiver email/s you entered were not found in the corpus')
            else:
                receivers.sort()  # Sorting receiver emails alphabetically

                # Filtering only terms which are in the database terms table
                cursor.execute(postgres_select_query2, (wordsInTuple,))
                rows2 = cursor.fetchall()
                for row in rows2:
                    words.append(row[0])
                if len(words) == 0:
                    print('The word/s you entered were not found in the corpus')
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
                        cursor.execute(postgres_select_query3, (wordsTuple, receiver, wordsTuple, startDate, endDate))
                        rows3 = cursor.fetchall()
                        for row in rows3:
                            words_Frequencies_per_receiver.append(row[1])
                        frequencies.append(words_Frequencies_per_receiver)

                    trace = go.Heatmap(x=words,
                                       y=receivers,
                                       z=frequencies,
                                       xgap=5,
                                       ygap=5,
                                       colorscale='Reds')
                    data = [trace]
                    layout = go.Layout(
                        xaxis=dict(
                            title='SPECIFIED TERMS (DATE RANGE BETWEEN' + str(months[startMonth - 1]) + ' ' + str(
                                startDay) + ' ' + str(startYear) + ' AND ' + str(months[endMonth - 1]) + ' ' + str(
                                endDay) + ' ' + str(endYear) + ')',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=True,
                            automargin=True,
                            # fixedrange = True,
                            showticklabels=True,
                            tickangle=45
                        ),
                        yaxis=dict(
                            title='SPECIFIED RECEIVERS',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=False,
                            automargin=True,
                            # fixedrange=True,
                            showticklabels=True,
                        )
                    )
                    fig = go.Figure(data=data, layout=layout)
                    py.plot(fig, filename='Receivers_Words_Frequency_Within_Time_Period')

    #What words did specified senders send most or specified receivers receive most within a specified date range?
    #HeatMap
    def Senders_Most_Popular_Words_Within_Time_Period(senderEmailList, startYear, startMonth, startDay, endYear, endMonth, endDay):
        if len(senderEmailList) == 0:
            print('You did not enter any sender emails!')
        else:
            senderEmailsTuple = tuple(senderEmailList)
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
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
                print('The sender email/s you entered were not found in the corpus')
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
                    print('No words have been found by these senders in the specified date range')

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
                        cursor.execute(postgres_select_query3, (wordsTuple, sender, wordsTuple, startDate, endDate,))
                        rows3 = cursor.fetchall()
                        for row in rows3:
                            words_Frequencies_per_sender.append(row[1])
                        frequencies.append(words_Frequencies_per_sender)

                    trace = go.Heatmap(x=words,
                                       y=senders,
                                       z=frequencies,
                                       xgap=5,
                                       ygap=5,
                                       colorscale='Reds')
                    data = [trace]
                    layout = go.Layout(
                        width=1400,
                        height=600,
                        xaxis=dict(
                            title='MOST POPULAR TERMS FOR THESE SENDERS BETWEEN ' + str(
                                months[startMonth - 1]) + ' ' + str(
                                startDay) + ' ' + str(startYear) + ' AND ' + str(months[endMonth - 1]) + ' ' + str(
                                endDay) + ' ' + str(endYear),
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=True,
                            automargin=True,
                            # fixedrange = True,
                            showticklabels=True,
                            tickangle=45
                        ),
                        yaxis=dict(
                            title='SPECIFIED SENDERS',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=False,
                            automargin=True,
                            # fixedrange=True,
                            showticklabels=True,
                        )
                    )
                    fig = go.Figure(data=data, layout=layout)
                    py.plot(fig, filename='Senders_Most_Popular_Words_Within_Time_Period')
    #HeatMap
    def Receivers_Most_Popular_Words_Within_Time_Period(receiverEmailList, startYear, startMonth, startDay, endYear, endMonth, endDay):
        if len(receiverEmailList) == 0:
            print('You did not enter any reciever emails!')
        else:
            receiverEmailsTuple = tuple(receiverEmailList)
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
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
                print('The receiver email/s you entered were not found in the corpus')
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
                    print('No words sent to these receivers have been found in the specified date range')

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
                        cursor.execute(postgres_select_query3, (wordsTuple, receiver, wordsTuple, startDate, endDate,))
                        rows3 = cursor.fetchall()
                        for row in rows3:
                            words_Frequencies_Per_Receiver.append(row[1])
                        frequencies.append(words_Frequencies_Per_Receiver)

                    trace = go.Heatmap(x=words,
                                       y=receivers,
                                       z=frequencies,
                                       xgap=5,
                                       ygap=5,
                                       colorscale='Reds')
                    data = [trace]
                    layout = go.Layout(
                        width=1400,
                        height=600,
                        xaxis=dict(
                            title='MOST POPULAR TERMS FOR THESE RECEIVERS BETWEEN ' + str(
                                months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear) + ' AND ' + str(
                                months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=True,
                            automargin=True,
                            # fixedrange = True,
                            showticklabels=True,
                            tickangle=45
                        ),
                        yaxis=dict(
                            title='SPECIFIED RECEIVERS',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=False,
                            automargin=True,
                            # fixedrange=True,
                            showticklabels=True,
                        )
                    )
                    fig = go.Figure(data=data, layout=layout)
                    py.plot(fig, filename='Receivers_Most_Popular_Words_Within_Time_Period')



    #CORRESPONDENCE HEATMAPS(SENDERS VS RECEIVERS)
    #Basic
    #HeatMap
    def correspondence_Heatmap_overall(emails_list):
        if len(emails_list) == 0:
            print('You did  not enter any sender emails!')
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
                print('No sender email you entered was found in the corpus')
            else:
                senderemails.sort()  # Sorting the sender emails alphabetically
                print(senderemails)

                # This is the query to get the top 4 recipients of a particular sender
                postgres_select_query2 = "\
                    SELECT p1.emailaddress AS sender, r.emailaddress AS recievers, COUNT(r.emailaddress) AS emailssent\
                    FROM enron.emails e INNER JOIN enron.people p1 ON e.senderid = p1.personid\
                    INNER JOIN (enron.recipients INNER JOIN enron.people ON recipients.personid = people.personid) r ON e.emailid = r.emailid\
                    WHERE p1.emailaddress = %s\
                   GROUP BY p1.emailaddress, r.emailaddress\
                   ORDER BY COUNT(r.emailaddress) DESC\
                   LIMIT 4;"

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

                print(receiverEmailsTuple)

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

                trace = go.Heatmap(x=recieveremails,
                                   y=senderemails,
                                   z=numberOfEmailsSent,
                                   xgap=5,
                                   ygap=5,
                                   colorscale='Reds')

                data = [trace]
                layout = go.Layout(
                    width=1400,
                    height=600,
                    xaxis=dict(
                        title='SENDERS\' MOST POPULAR RECEIVERS',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=True,
                        automargin=True,
                        # fixedrange = True,
                        showticklabels=True,
                        tickangle=45
                    ),
                    yaxis=dict(
                        title='SPECIFIED SENDERS',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=False,
                        automargin=True,
                        # fixedrange=True,
                        showticklabels=True,
                    )
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='correspondence_Heatmap_overall')

    #Within specified date range
    #HeatMap
    def correspondence_Heatmap_Within_Time_Period(emails_list, startYear, startMonth, startDay, endYear, endMonth, endDay):
        if len(emails_list) == 0:
            print('You did  not enter any sender emails!')
        else:
            emails_tuple = tuple(emails_list)
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)
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
                print('No sender email you entered was found in the corpus!')
            else:
                senderemails.sort()  # Sorting the sender emails alphabetically
                print(senderemails)

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

                print(receiverEmailsTuple)
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

                trace = go.Heatmap(x=recieveremails,
                                   y=senderemails,
                                   z=numberOfEmailsSent,
                                   xgap=5,
                                   ygap=5,
                                   colorscale='Reds')

                data = [trace]
                layout = go.Layout(
                    width=1400,
                    height=600,
                    xaxis=dict(
                        title='SENDERS\' MOST POPULAR RECEIVERS WITHIN THE DATE RANGE ' + str(
                            months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear) + ' AND ' + str(
                            months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=True,
                        automargin=True,
                        # fixedrange = True,
                        showticklabels=True,
                        tickangle=45
                    ),
                    yaxis=dict(
                        title='SPECIFIED SENDERS',
                        autorange=True,
                        showgrid=True,
                        zeroline=True,
                        showline=False,
                        automargin=True,
                        # fixedrange=True,
                        showticklabels=True,
                    )
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='correspondence_Heatmap_Within_Time_Period')

    #With particular terms
    #HeatMap
    def correspondence_Heatmap_With_Particular_Terms(emails_list, termsInList):
        if len(emails_list) == 0:
            print('You did not enter any sender emails!')
        elif len(termsInList) == 0:
            print('You did not enter any terms!')
        else:
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
                print('No sender email you entered was found in the corpus')
            else:
                senderemails.sort()  # Sorting the sender emails alphabetically
                print(senderemails)

                # Filtering out only terms which are in the database's terms table
                postgres_select_query2 = "SELECT term FROM enron.terms WHERE term IN %s"
                cursor.execute(postgres_select_query2, (termsInTuple,))
                rows2 = cursor.fetchall()
                for row in rows2:
                    termsList.append(row[0])

                if len(termsList) == 0:
                    print('No term you entered was found in the corpus')
                else:
                    termsList.sort()  # Sorting the sender emails alphabetically
                    termsTuple = tuple(termsList)
                    print(termsTuple)

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

                    print(receiverEmailsTuple)

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

                    trace = go.Heatmap(x=recieveremails,
                                       y=senderemails,
                                       z=numberOfEmailsSent,
                                       xgap=5,
                                       ygap=5,
                                       colorscale='Reds')

                    data = [trace]
                    layout = go.Layout(
                        width=1400,
                        height=600,
                        xaxis=dict(
                            title='SENDERS\' MOST POPULAR RECEIVERS: WITH WORDS ' + str(termsTuple),
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=True,
                            automargin=True,
                            # fixedrange = True,
                            showticklabels=True,
                            tickangle=45
                        ),
                        yaxis=dict(
                            title='SPECIFIED SENDERS',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=False,
                            automargin=True,
                            # fixedrange=True,
                            showticklabels=True,
                        )
                    )
                    fig = go.Figure(data=data, layout=layout)
                    py.plot(fig, filename='correspondence_Heatmap_With_Particular_Terms')

    #With particular terms within specified date range
    #HeatMap
    def correspondence_Heatmap_With_Particular_Terms_Within_Time_Period(emails_list, termsInList, startYear, startMonth, startDay, endYear, endMonth, endDay):
        if len(emails_list) == 0:
            print('You did not enter any sender emails!')
        elif len(termsInList) == 0:
            print('You did not enter any terms!')
        else:
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)
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
                print('No sender email you entered was found in the corpus')
            else:
                senderemails.sort()  # Sorting the sender emails alphabetically
                print(senderemails)

                # Filtering out only terms which are in the database's terms table
                postgres_select_query2 = "SELECT term FROM enron.terms WHERE term IN %s"
                cursor.execute(postgres_select_query2, (termsInTuple,))
                rows2 = cursor.fetchall()
                for row in rows2:
                    termsList.append(row[0])

                if len(termsList) == 0:
                    print('No term you entered was found in the corpus')
                else:
                    termsList.sort()  # Sorting the sender emails alphabetically
                    termsTuple = tuple(termsList)
                    print(termsTuple)

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

                    print(receiverEmailsTuple)

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

                    trace = go.Heatmap(x=recieveremails,
                                       y=senderemails,
                                       z=numberOfEmailsSent,
                                       xgap=5,
                                       ygap=5,
                                       colorscale='Reds')

                    data = [trace]
                    layout = go.Layout(
                        width=1400,
                        height=600,
                        xaxis=dict(
                            title='SENDERS\' MOST POPULAR RECEIVERS: WITH WORDS ' + str(
                                termsTuple) + ' WITHIN THE DATE RANGE ' + str(
                                months[startMonth - 1]) + ' ' + str(startDay) + ' ' + str(startYear) + ' AND ' + str(
                                months[endMonth - 1]) + ' ' + str(endDay) + ' ' + str(endYear),
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=True,
                            automargin=True,
                            # fixedrange = True,
                            showticklabels=True,
                            tickangle=45
                        ),
                        yaxis=dict(
                            title='SPECIFIED SENDERS',
                            autorange=True,
                            showgrid=True,
                            zeroline=True,
                            showline=False,
                            automargin=True,
                            # fixedrange=True,
                            showticklabels=True,
                        )
                    )
                    fig = go.Figure(data=data, layout=layout)
                    py.plot(fig, filename='correspondence_Heatmap_With_Particular_Terms_Within_Time_Period')



    #CALENDAR HEATMAPS(VOLUME OF EMAILS SENT)
    #Basic
    #HeatMap
    def calendar_heatmap_for_year(year):
        if year < 1900 or year > 2019:
            print('The year you entered is invalid!')
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

            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
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

            trace = go.Heatmap(z=numOfEmails, y=ydata, x=xdata, hoverinfo='text', xgap=5, ygap=5, colorscale='Reds',text=hovertext)
            data = [trace]

            layout = go.Layout(
                width=1400,
                height=600,
                title="Volume of Emails Sent per Day in the Year " + str(year),
                xaxis={
                    "ticklen": 0,
                    "tickmode": "array",
                    "ticktext": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
                    "tickvals": [1, 6, 10, 14, 18, 23, 27, 33, 36, 40, 45, 49],
                    "title": "Months in the Year " + str(year),
                    "autorange": True,
                    "automargin": True,
                    "showticklabels": True
                },
                yaxis={
                    "tickmode": "array",
                    "ticktext": ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
                    "tickvals": [1, 2, 3, 4, 5, 6, 7],
                    "title": "Days of the Week",
                    "autorange": True,
                    "automargin": True,
                    "showticklabels": True
                }
            )
            fig = go.Figure(data=data, layout=layout)
            py.plot(fig, filename='calendar_heatmap_for_year')

    #With respect to specified people
    #HeatMap
    def calendar_heatmap_for_year_with_particular_senders(year, sendersListIn):
        if year < 1900 or year > 2019:
            print('The year you entered is invalid!')
        elif len(sendersListIn) == 0:
            print('You did not enter any senders!')
        else:
            startDate = str(year) + "-01-01"
            endDate = str(year) + "-12-31"
            sendersTupleIn = tuple(sendersListIn)
            senders = []
            leapYrs = [1996, 2000, 2004, 2008]
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
            daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

            # Filtering out only email addresses which are in the database's people table
            postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s"
            cursor.execute(postgres_select_query1, (sendersTupleIn,))
            rows1 = cursor.fetchall()
            for row in rows1:
                senders.append(row[0])

            if len(senders) == 0:
                print('No sender email you entered was found in the corpus!')
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

                trace = go.Heatmap(z=numOfEmails, y=ydata, x=xdata, hoverinfo='text', xgap=5, ygap=5, colorscale='Reds',text=hovertext)
                data = [trace]

                layout = go.Layout(
                    width=1400,
                    height=600,
                    title="Volume of Emails Sent by " + str(senders) + " in the Year " + str(year),
                    xaxis={
                        "ticklen": 0,
                        "tickmode": "array",
                        "ticktext": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
                        "tickvals": [1, 6, 10, 14, 18, 23, 27, 33, 36, 40, 45, 49],
                        "title": "Months in the Year " + str(year),
                        "autorange" : True,
                        "automargin" : True,
                        "showticklabels" :True
                    },
                    yaxis={
                        "tickmode": "array",
                        "ticktext": ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
                        "tickvals": [1, 2, 3, 4, 5, 6, 7],
                        "title": "Days of the Week",
                        "autorange": True,
                        "automargin": True,
                        "showticklabels": True
                    }
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='calendar_heatmap_for_year_with_particular_senders')
    #HeatMap
    def calendar_heatmap_for_year_with_particular_receivers(year, receiversInList):
        if year < 1900 or year > 2019:
            print('The year you entered is invalid!')
        elif len(receiversInList) == 0:
            print('You did not enter any senders!')
        else:
            startDate = str(year) + "-01-01"
            endDate = str(year) + "-12-31"
            receiversTupleIn = tuple(receiversInList)
            receivers = []
            leapYrs = [1996, 2000, 2004, 2008]
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
            daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

            # Filtering out only email addresses which are in the database's people table
            postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress IN %s"
            cursor.execute(postgres_select_query1, (receiversTupleIn,))
            rows1 = cursor.fetchall()
            for row in rows1:
                receivers.append(row[0])

            if len(receivers) == 0:
                print('No receiver email you entered was found in the corpus!')
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

                trace = go.Heatmap(z=numOfEmails, y=ydata, x=xdata, hoverinfo='text', xgap=5, ygap=5, colorscale='Reds',text=hovertext)
                data = [trace]

                layout = go.Layout(
                    width=1400,
                    height=600,
                    title="Volume of Emails Received by " + str(receivers) + " in the Year " + str(year),
                    xaxis={
                        "ticklen": 0,
                        "tickmode": "array",
                        "ticktext": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
                        "tickvals": [1, 6, 10, 14, 18, 23, 27, 33, 36, 40, 45, 49],
                        "title": "Months in the Year " + str(year),
                        "autorange" : True,
                        "automargin" : True,
                        "showticklabels" :True
                    },
                    yaxis={
                        "tickmode": "array",
                        "ticktext": ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
                        "tickvals": [1, 2, 3, 4, 5, 6, 7],
                        "title": "Days of the Week",
                        "autorange": True,
                        "automargin": True,
                        "showticklabels": True
                    }
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='calendar_heatmap_for_year_with_particular_receivers')

    #With respect to specified terms
    #HeatMap
    def calendar_heatmap_for_year_with_particular_terms(year, termsListIn):
        if year < 1900 or year > 2019:
            print('The year you entered is invalid!')
        elif len(termsListIn) == 0:
            print('You did not enter any terms!')
        else:
            startDate = str(year) + "-01-01"
            endDate = str(year) + "-12-31"
            termsTupleIn = tuple(termsListIn)
            terms = []
            leapYrs = [1996, 2000, 2004, 2008]
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
            daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

            # Filtering out only terms which are in the database's terms table
            postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"
            cursor.execute(postgres_select_query1, (termsTupleIn,))
            rows1 = cursor.fetchall()
            for row in rows1:
                terms.append(row[0])

            if len(terms) == 0:
                print('No term you entered was found in the corpus!')
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

                trace = go.Heatmap(z=numOfEmails, y=ydata, x=xdata, hoverinfo='text', xgap=5, ygap=5, colorscale='Reds',
                                   text=hovertext)
                data = [trace]

                layout = go.Layout(
                    width=1400,
                    height=600,
                    title="Volume of Emails Sent Containing One or More of the Words " + str(
                        terms) + " in the Year " + str(year),
                    xaxis={
                        "ticklen": 0,
                        "tickmode": "array",
                        "ticktext": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
                                     "Dec"],
                        "tickvals": [1, 6, 10, 14, 18, 23, 27, 33, 36, 40, 45, 49],
                        "title": "Months in the Year " + str(year),
                        "autorange": True,
                        "automargin": True,
                        "showticklabels": True
                    },
                    yaxis={
                        "tickmode": "array",
                        "ticktext": ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
                        "tickvals": [1, 2, 3, 4, 5, 6, 7],
                        "title": "Days of the Week",
                        "autorange": True,
                        "automargin": True,
                        "showticklabels": True
                    }
                )
                fig = go.Figure(data=data, layout=layout)
                py.plot(fig, filename='calendar_heatmap_for_year_with_particular_terms')

    #With respect to specified people and specified terms
    #HeatMap
    def calendar_heatmap_for_year_with_particular_senders_receivers_terms(year, sendersListIn, receiversListIn, termsListIn):
        if year < 1900 or year > 2019:
            print('The year you entered is invalid!')
        elif len(termsListIn) == 0:
            print('You did not enter any terms!')
        elif len(sendersListIn) == 0:
            print('You did not enter any sender emails!')
        elif len(receiversListIn) == 0:
            print('You did not enter any receiver emails!')
        else:
            startDate = str(year) + "-01-01"
            endDate = str(year) + "-12-31"
            termsTupleIn = tuple(termsListIn)
            terms = []
            sendersTupleIn = tuple(sendersListIn)
            senders = []
            receiversTupleIn = tuple(receiversListIn)
            receivers = []
            leapYrs = [1996, 2000, 2004, 2008]
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                      'November', 'December']
            daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

            postgres_select_query1 = "SELECT term FROM enron.terms WHERE term IN %s"

            # Filtering out only terms which are in the database's terms table
            cursor.execute(postgres_select_query1, (termsTupleIn,))
            rows1 = cursor.fetchall()
            for row in rows1:
                terms.append(row[0])

            if len(terms) == 0:
                print('No term you entered was found in the corpus!')
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
                    print('No sender email you entered was found in the corpus!')
                else:
                    senders.sort()
                    sendersTuple = tuple(senders)

                    # Filtering out only receiver emails which are in the database's people table
                    cursor.execute(postgres_select_query2, (receiversTupleIn,))
                    rows3 = cursor.fetchall()
                    for row in rows3:
                        receivers.append(row[0])

                    if len(receivers) == 0:
                        print('No receiver email you entered was found in the corpus!')
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

                        trace = go.Heatmap(z=numOfEmails, y=ydata, x=xdata, hoverinfo='text', xgap=5, ygap=5,
                                           colorscale='Reds', text=hovertext)
                        data = [trace]

                        layout = go.Layout(
                            width=1400,
                            height=600,
                            title="Volume of Emails Sent in the Year " + str(
                                year) + " Containing One or More of the Words " + str(terms) +
                                  '\n where Sender is One from ' + str(senders) +
                                  '\n and Receivers are One or More from' + str(receivers),
                            xaxis={
                                "ticklen": 0,
                                "tickmode": "array",
                                "ticktext": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct",
                                             "Nov", "Dec"],
                                "tickvals": [1, 6, 10, 14, 18, 23, 27, 33, 36, 40, 45, 49],
                                "title": "Months in the Year " + str(year),
                                "autorange": True,
                                "automargin": True,
                                "showticklabels": True
                            },
                            yaxis={
                                "tickmode": "array",
                                "ticktext": ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
                                "tickvals": [1, 2, 3, 4, 5, 6, 7],
                                "title": "Days of the Week",
                                "autorange": True,
                                "automargin": True,
                                "showticklabels": True
                            }
                        )
                        fig = go.Figure(data=data, layout=layout)
                        py.plot(fig, filename='calendar_heatmap_for_year_with_particular_senders_receivers_terms')



    #WORDCLOUDS
    #Most common terms in specified date range
    #Returns JSON
    def WordCloud_Most_Common_Terms_Within_Period_Of_Time(startYear, startMonth, startDay, endYear, endMonth, endDay):
        startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
        endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)
        termFreq = []
        terms = []
        freqs = []

        query = '''
            SELECT SUM(et.frequency), t.term FROM enron.emailterms AS et
            INNER JOIN enron.terms AS t ON t.termid = et.termid
            INNER JOIN enron.emails AS e ON e.emailid = et.emailid
            WHERE LENGTH(t.term) > 3 AND e.date BETWEEN %s AND %s
            GROUP BY t.term 
            ORDER BY SUM(et.frequency) DESC 
            LIMIT 50;
        '''

        cursor.execute(query, (startDate, endDate,))
        records = cursor.fetchall()
        for row in records:
            termFreq.append([row[0], row[1]])

        random.shuffle(termFreq) #Shuffling terms, but not losing the relationship between term and frequency

        for x in termFreq:
            freqs.append(x[0])
            terms.append(x[1])

        combined = [terms, freqs]
        JSONSTRING = json.dumps(combined)
        print(JSONSTRING)

    #Most common terms between a specified set of senders and a specified set of receivers within specified date range
    #Returns JSON
    def WordCloud_Most_Common_Terms_Between_Senders_And_Receivers_Within_Period_Of_Time(sendersListIn, receiversListIn, startYear, startMonth, startDay, endYear, endMonth, endDay):
        if len(sendersListIn) == 0:
            print('You did not enter any senders!')
        elif len(receiversListIn) == 0:
            print('You did not enter any receivers!')
        else:
            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)
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
                print('No sender email you entered was found in the corpus!')
            else:
                sendersTuple = tuple(senders)

                # Filtering out only receiver email addresses which are in the database's people table
                cursor.execute(postgres_select_query1, (receiversTupleIn,))
                rows1 = cursor.fetchall()
                for row in rows1:
                    receivers.append(row[0])

                if len(receivers) == 0:
                    print('No receiver email you entered was found in the corpus!')
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
                GROUP BY t.term
                ORDER BY SUM(et.frequency) DESC
                LIMIT 50;
            '''

            cursor.execute(postgres_select_query2, (sendersTuple, receiversTuple, startDate, endDate,))
            records = cursor.fetchall()
            for row in records:
                termFreq.append([row[0], row[1]])

            random.shuffle(termFreq) #Shuffling terms, but not losing the relationship between term and frequency

            for x in termFreq:
                freqs.append(x[0])
                terms.append(x[1])

            combined = [terms, freqs]
            JSONSTRING = json.dumps(combined)
            print(JSONSTRING)



    #EMAIL FILTERING FUNCTIONALITY
    #Filter by date
    #Returns JSON
    def Get_Email_Data_Filter_By_Just_Date(startYear, startMonth, startDay, endYear, endMonth, endDay):

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)
            jsons_list = []  # To hold the returned jsons from the database

            #This query will get all the email data (except email body) as JSONs
            postgres_select_query = '''
                SELECT row_to_json(r)
                FROM(
                SELECT p.emailaddress AS sender, array_agg(r.emailaddress) as "receiver/s",e.date::date as date, e.subject as subject, e.numofwords AS "numberOfWords" , e.directory as "directoryAtSource", e.emailid as "emailID"
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

            print(jsons_list)

    #Filter by people
    #Returns JSON
    def Get_Email_Data_Filter_By_Sender(senderEmailAddress, startYear, startMonth, startDay, endYear, endMonth, endDay):
        #First we check if the email address sent in is in the database's people table
        postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress = %s"

        cursor.execute(postgres_select_query1, (senderEmailAddress,))
        row = cursor.fetchone()
        if row == None:
            print('The email you entered is not in the corpus!')
        else:
            sender = row[0]

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)
            jsons_list = []  # To hold the returned jsons from the database

            #This query will get all the email data (except email body) of the specified sender as JSONs
            postgres_select_query2 = '''
                SELECT row_to_json(r)
                FROM(
                SELECT p.emailaddress AS sender, array_agg(r.emailaddress) as "receiver/s",e.date::date as date, e.subject as subject, e.numofwords AS "numberOfWords" , e.directory as "directoryAtSource", e.emailid as "emailID"
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

            print(jsons_list)
    #Returns JSON
    def Get_Email_Data_Filter_By_Receiver(receiverEmailAddress, startYear, startMonth, startDay, endYear, endMonth, endDay):
        #First we check if the email address sent in is in the database's people table
        postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress = %s"

        cursor.execute(postgres_select_query1, (receiverEmailAddress,))
        row = cursor.fetchone()
        if row == None:
            print('The email you entered is not in the corpus!')
        else:
            receiver = row[0]

            startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
            endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)
            jsons_list = []  # To hold the returned jsons from the database

            #This query will get all the email data (except email body) of the specified receiver as JSONs
            postgres_select_query2 = '''
                SELECT row_to_json(r)
                FROM(
                SELECT p.emailaddress AS sender, array_agg(r.emailaddress) as "receiver/s",e.date::date as date, e.subject as subject, e.numofwords AS "numberOfWords" , e.directory as "directoryAtSource", e.emailid as "emailID"
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

            print(jsons_list)
    #Returns JSON
    def Get_Email_Data_Filter_By_Sender_And_Receiver(senderEmailAddress, receiverEmailAddress, startYear, startMonth, startDay, endYear, endMonth, endDay):
        #First we check if the email addresses sent in are in the database's people table
        postgres_select_query1 = "SELECT emailaddress FROM enron.people WHERE emailaddress = %s"

        cursor.execute(postgres_select_query1, (senderEmailAddress,))
        row = cursor.fetchone()
        if row == None:
            print('The sender email you entered is not in the corpus!')
        else:
            sender = row[0]
            cursor.execute(postgres_select_query1, (receiverEmailAddress,))
            row = cursor.fetchone()
            if row == None:
                print('The receiver email you entered is not in the corpus!')
            else:
                receiver = row[0]

                startDate = str(startYear) + '-' + str(startMonth) + '-' + str(startDay)
                endDate = str(endYear) + '-' + str(endMonth) + '-' + str(endDay)
                jsons_list = []  # To hold the returned jsons from the database

                #This query will get all the email data (except email body) of the specified sender and receiver as JSONs
                postgres_select_query2 = '''
                    SELECT row_to_json(r)
                    FROM(
                    SELECT p.emailaddress AS sender, array_agg(r.emailaddress) as "receiver/s",e.date::date as date, e.subject as subject, e.numofwords AS "numberOfWords" , e.directory as "directoryAtSource", e.emailid as "emailID"
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

                print(jsons_list)

    #Returns full details of particular email(when user clicks on an email shown through one of the functions above)
    #Returns JSON
    def Get_Full_Email_Details_By_EmailID(emailIDin):
        #First we have to ensure that the emailid received exists
        postgres_select_query1 = "SELECT emailID FROM enron.emails WHERE emailid = %s"

        cursor.execute(postgres_select_query1, (emailIDin,))
        row = cursor.fetchone()
        if row == None:
            print('The emailid you entered is not in the corpus!')
        else:
            emailID = row[0]

            json_list = []  # To hold the returned json from the database

            #This query will get all the email data (INCLUDING the email body) as JSONs
            postgres_select_query2 = '''
                SELECT row_to_json(r)
                FROM(
                SELECT p.emailaddress AS sender, array_agg(r.emailaddress) as "receiver/s",e.date::date as date, e.subject as subject, eft.emailtext ,e.numofwords AS "numberOfWords" , e.directory as "directoryAtSource", e.emailid as "emailID"
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
            json_list.append(row[0])

            print(json_list)
            print(len(json_list))

    # END. OF. FUNCTIONS.

    # START. OF. FUNCTION. CALLS.

    #Most_Popular_Words_In_Particular_Year_Grouped_By_Month(2001)
    #Most_Popular_Words_In_Particular_Month_Grouped_By_Day(2001, 4)
    #Most_Popular_Words_In_Particular_Day(2001, 4, 9)
    #Most_Popular_Years_Words_Were_Mentioned(['bankruptcy','bankrupt', 'management', 'crisis', 'news'])
    #Most_Popular_Months_Words_Were_Mentioned(['bankruptcy','bankrupt', 'management', 'crisis', 'news'])
    #Most_Popular_Days_Words_Were_Mentioned(['power','electricity','bankruptcy','bankrupt','time'])
    #Word_Mentions_In_Year_Grouped_By_Month(['bankruptcy', 'management', 'crisis', 'news', 'report', 'manager', 'business'],2000)
    #Word_Mentions_In_Month_Grouped_By_Day(['bankruptcy', 'management', 'crisis', 'news', 'report', 'manager', 'business'], 2000, 2)
    #Word_Mentions_In_Particular_Day(['bankruptcy', 'management', 'crisis', 'news', 'report', 'manager', 'business'], 2002, 1, 30)
    #three_term_frequency_comparative_analysis_over_single_year(['crisis', 'bankruptcy', 'news'], 2002)
    #three_term_frequency_comparative_analysis_over_single_month(['crisis', 'bankruptcy', 'news'], 2001, 12)
    #three_year_volume_of_emails_sent_with_particular_terms_comparative_analysis(2000, 2001, 2002, ['crisis', 'bankruptcy', 'emergency', 'danger', 'bankrupt'])
    #three_month_volume_of_emails_sent_with_particular_terms_comparative_analysis(2001, 4, 2001, 10, 2001, 12, ['crisis', 'bankruptcy', 'emergency', 'danger', 'bankrupt'])

    #Most_Frequent_Senders_Within_Specified_Date_Range(2001,12,1,2002,1,31)
    #Most_Frequent_Receivers_Within_Specified_Date_Range(2001,12,1,2002,1,31)
    #How_Many_Emails_Senders_Sent_Within_Time_Period(['jmunoz@mcnallytemple.com','miyung.buster@enron.com','jeff.dasovich@enron.com','ann.schmidt@enron.com','m..schmidt@enron.com','kristin.walsh@enron.com','chairman.enron@enron.com','central.communication@enron.com','nytdirect@nytimes.com','coo.jeff@enron.com'],2001,12,1,2002,1,31)
    #How_Many_Emails_Receivers_Received_Within_Time_Period(['jeff.dasovich@enron.com','richard.shapiro@enron.com','sally.beck@enron.com','susan.mara@enron.com','kenneth.lay@enron.com','paul.kaufman@enron.com'],2001,10,1,2002,1,31)

    #Most_Frequent_Senders_Of_Words_Within_Time_Period(['problem', 'bankruptcy', 'scandal', 'press'],2002,1,1,2002,1,31)
    #Most_Frequent_Receivers_Of_Words_Within_Time_Period(['bankrupt','bankruptcy', 'news', 'power', 'energy'],2001,10,20,2002,1,31)
    #Senders_Words_Frequency_Within_Time_Period(['jmunoz@mcnallytemple.com','miyung.buster@enron.com','jeff.dasovich@enron.com','ann.schmidt@enron.com','m..schmidt@enron.com','kristin.walsh@enron.com','chairman.enron@enron.com','central.communication@enron.com','nytdirect@nytimes.com','coo.jeff@enron.com'],['emergency','crisis','bankrupt','bankruptcy', 'panic', 'management'],2001,12,1,2002,1,31)
    #Receivers_Words_Frequency_Within_Time_Period(['jeff.dasovich@enron.com','richard.shapiro@enron.com','sally.beck@enron.com','susan.mara@enron.com','kenneth.lay@enron.com','paul.kaufman@enron.com'],['emergency', 'crisis', 'bankrupt', 'bankruptcy', 'press', 'news', 'management'],2001,10,1,2002,1,31)
    #Senders_Most_Popular_Words_Within_Time_Period(['chairman.enron@enron.com','shelley.corman@enron.com','no.address@enron.com'],2001,12,1,2002,1,31)
    #Receivers_Most_Popular_Words_Within_Time_Period(['jeff.dasovich@enron.com','richard.shapiro@enron.com','sally.beck@enron.com','susan.mara@enron.com','kenneth.lay@enron.com','paul.kaufman@enron.com'],2001,10,1,2002,1,31)

    #correspondence_Heatmap_overall(['sally.beck@enron.com', 'john.arnold@enron.com', 'michelle.cash@enron.com', 'larry.campbell@enron.com', 'outlook.team@enron.com', 'rick.buy@enron.com','susan.mara@enron.com', 'rosalee.fleming@enron.com','shelley.corman@enron.com','sherri.sera@enron.com', 'james.steffes@enron.com','ginger.dernehl@enron.com','owner-eveningmba@haas.berkeley.edu', 'cameron@perfect.com', 'robert.badeer@enron.com', 'jbennett@gmssr.com', 'janel.guerrero@enron.com','rhonda.denton@enron.com', 'steven.kean@enron.com','mona.petrochko@enron.com','d..steffes@enron.com'])
    #correspondence_Heatmap_Within_Time_Period(['sally.beck@enron.com', 'john.arnold@enron.com', 'michelle.cash@enron.com', 'larry.campbell@enron.com', 'outlook.team@enron.com', 'rick.buy@enron.com','susan.mara@enron.com', 'rosalee.fleming@enron.com','shelley.corman@enron.com','sherri.sera@enron.com', 'james.steffes@enron.com','ginger.dernehl@enron.com','owner-eveningmba@haas.berkeley.edu', 'cameron@perfect.com', 'robert.badeer@enron.com', 'jbennett@gmssr.com', 'janel.guerrero@enron.com','rhonda.denton@enron.com', 'steven.kean@enron.com','mona.petrochko@enron.com','d..steffes@enron.com'],2001,10,1,2002,1,31)
    #correspondence_Heatmap_With_Particular_Terms(['rick.buy@enron.com', 'sally.beck@enron.com', 'shelley.corman@enron.com','kenneth.lay@enron.com','susan.mara@enron.com', 'rosalee.fleming@enron.com'], ['bankruptcy', 'power', 'enron' ,'energy', 'electricity', 'bankrupt'])
    #correspondence_Heatmap_With_Particular_Terms_Within_Time_Period(['rick.buy@enron.com', 'sally.beck@enron.com', 'shelley.corman@enron.com', 'susan.mara@enron.com', 'rosalee.fleming@enron.com'], ['bankruptcy', 'power', 'enron' ,'energy', 'electricity', 'bankrupt'],2001,10,1,2002,1,31)

    #calendar_heatmap_for_year(2001)
    #calendar_heatmap_for_year_with_particular_senders(2001,['sdfdsdf','sdfsdfdsfsdfsdf','susan.mara@enron.com', 'jeff.dasovich@enron.com'])
    #calendar_heatmap_for_year_with_particular_receivers(2001, ['dfasd','jeff.dasovich@enron.com', 'susan.mara@enron.com','asdaseeasd@asfzsz.com'])
    #calendar_heatmap_for_year_with_particular_terms(2001, ['bankrupt', 'management', 'crisis', 'bankruptcy', 'emergency'])
    #calendar_heatmap_for_year_with_particular_senders_receivers_terms(2001, ['jeff.dasovich@enron.com','richard.shapiro@enron.com','sally.beck@enron.com','susan.mara@enron.com','kenneth.lay@enron.com','paul.kaufman@enron.com'], ['rick.buy@enron.com', 'sally.beck@enron.com', 'shelley.corman@enron.com', 'susan.mara@enron.com', 'rosalee.fleming@enron.com'], ['bankrupt', 'management', 'crisis', 'bankruptcy', 'emergency'])

    #WordCloud_Most_Common_Terms_Within_Period_Of_Time(2001,1,1,2002,12,31)
    #WordCloud_Most_Common_Terms_Between_Senders_And_Receivers_Within_Period_Of_Time(['paul.kaufman@enron.com','leslie.lawner@enron.com','jeff.dasovich@enron.com', 'richard.shapiro@enron.com','jeff.skilling@enron.com', 'kenneth.lay@enron.com','susan.mara@enron.com','sally.beck@enron.com','james.steffes@enron.com'], ['paul.kaufman@enron.com','leslie.lawner@enron.com','jeff.dasovich@enron.com', 'richard.shapiro@enron.com','jeff.skilling@enron.com', 'kenneth.lay@enron.com','susan.mara@enron.com','sally.beck@enron.com','james.steffes@enron.com'], 2002,1,1,2002,1,31)

    #Get_Email_Data_Filter_By_Just_Date(2002, 1, 30, 2002, 1, 31)
    #Get_Email_Data_Filter_By_Sender('jeff.dasovich@enron.com', 2001, 12, 1, 2002, 1, 31)
    #Get_Email_Data_Filter_By_Receiver('jeff.dasovich@enron.com',2001,12,1,2002,1,31)
    #Get_Email_Data_Filter_By_Sender_And_Receiver('susan.mara@enron.com','jeff.dasovich@enron.com', 2001, 1, 1, 2001, 12, 31)
    #Get_Full_Email_Details_By_EmailID(74000)

    #END. OF. FUNCTION. CALLS.

except (Exception, psycopg2.Error) as error:
    print("ERROR IN DB OPERATION:", error)
finally:
    # closing database connection.
    if (connection):
        # Close cursor object and database connection
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")