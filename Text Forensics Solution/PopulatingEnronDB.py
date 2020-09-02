import psycopg2
import os
import nltk
import re
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from email.parser import Parser

try:

    connection = psycopg2.connect(user="postgres", password="1234", host="127.0.0.1", port="5432", database="EnronDB")
    cursor = connection.cursor()  # creating a cursor object using the connection object

    rootdir = "A:\mydata"
    my_parser = Parser()  # Used to parse the email files

    default_stopwords = set(nltk.corpus.stopwords.words('english'))
    custom_stopwords = {"nay", "``", "''", "'d", "'s", "n't", '...', '--', "'m", "'ll", "would"}
    all_stopwords = default_stopwords.union(custom_stopwords)

    lemmatizer = WordNetLemmatizer()  # instantiating lemmatizer


    def storeEmail(inputfile):
        with open(inputfile, "r") as file:  # Like this the file gets closed automatically
            data = file.read()  # getting the raw email text

        email = my_parser.parsestr(data)  # parsing the text

        # extracting the relevant stuff from the parsed email file and populating the database tables
        email_subject = email['subject']
        email_date = email['date']
        email_date = re.sub("[+-].*","",email_date)
        email_sender = email['from']
        email_receivers_list = []

        # An email can be sent to multiple people, so we have to seperate them and store them as separate people in our list
        if email['to']:  # if it is not None (like null in Java or C#)(some emails are in a non standard format, so python cant extract the 'to' part, and instead returns None)
            formatted_to_emails = email['to'].replace("\n", "")
            formatted_to_emails = formatted_to_emails.replace("\t", "")
            formatted_to_emails = formatted_to_emails.replace(" ", "")
            email_list = formatted_to_emails.split(",")  # returns a list
            for receiver in email_list:
                email_receivers_list.append(receiver)

        email_receivers = set(email_receivers_list)
        email_body = str(email.get_payload())

        # tokenizing the text
        tokens = nltk.word_tokenize(email_body)

        email_length = len(tokens)
        email_directory = re.sub(r".*mydata\\", "", inputfile)


        postgreSQL_select_Query1 = "SELECT * FROM enron.people WHERE emailaddress = %s"
        postgreSQL_insert_Query1 = "INSERT INTO enron.people(emailaddress, sentemails) VALUES (%s, %s)"
        postgreSQL_insert_Query2 = "INSERT INTO enron.people(emailaddress, receivedemails) VALUES (%s, %s)"
        postgreSQL_update_Query1 = "UPDATE enron.people SET sentemails = %s WHERE emailaddress = %s "
        postgreSQL_update_Query2 = "UPDATE enron.people SET receivedemails = %s WHERE emailaddress = %s "

        cursor.execute(postgreSQL_select_Query1, (email_sender,))
        row = cursor.fetchone()
        if row == None:
            cursor.execute(postgreSQL_insert_Query1,(email_sender,1,))
            connection.commit()  # commiting changes to the database.
        else:
            cursor.execute("SELECT sentemails FROM enron.people WHERE emailaddress = %s",(email_sender,))
            record = cursor.fetchone()
            updatedSentEmails = int(record[0]) + 1
            cursor.execute(postgreSQL_update_Query1,(updatedSentEmails, email_sender,))
            connection.commit()  # commiting changes to the database.

        for recipient in email_receivers:
            cursor.execute(postgreSQL_select_Query1, (recipient,))
            row = cursor.fetchone()
            if row == None:
                cursor.execute(postgreSQL_insert_Query2, (recipient,1,))
                connection.commit()  # commiting changes to the database
            else:
                cursor.execute("SELECT receivedemails FROM enron.people WHERE emailaddress = %s", (recipient,))
                record = cursor.fetchone()
                updatedRecievedEmails = int(record[0]) + 1
                cursor.execute(postgreSQL_update_Query2, (updatedRecievedEmails, recipient,))
                connection.commit()  # commiting changes to the database.

        postgreSQL_select_Query2 = "SELECT personid FROM enron.people WHERE emailaddress = %s"
        postgreSQL_insert_Query3 = "INSERT INTO enron.emails(subject, date, senderid, numofwords, directory) VALUES (%s,%s,%s,%s,%s)"

        sender_ID = 0
        cursor.execute(postgreSQL_select_Query2, (email_sender,))
        record = cursor.fetchone()
        sender_ID = record[0]

        cursor.execute(postgreSQL_insert_Query3, (email_subject,email_date,sender_ID,email_length,email_directory,))
        connection.commit()  # commiting changes to the database.

        postgreSQL_select_Query3 = "SELECT emailid FROM enron.emails WHERE directory = %s"
        cursor.execute(postgreSQL_select_Query3, (email_directory,))
        record = cursor.fetchone()
        email_ID = record[0]

        postgreSQL_insert_Query4 = "INSERT INTO enron.recipients VALUES (%s,%s)"
        postgreSQL_select_Query4 = "SELECT personid FROM enron.people WHERE emailaddress = %s"
        for recipient in email_receivers:
            cursor.execute(postgreSQL_select_Query4, (recipient,))
            record = cursor.fetchone()
            recipient_ID = record[0]
            cursor.execute(postgreSQL_insert_Query4,(email_ID,recipient_ID,))
            connection.commit()  # commiting changes to the database.

        #Now I will remove the stopwords and lemmatize
        # making all words lowercase
        tokens = [word.lower() for word in tokens]
        # Remove single-character tokens (mostly punctuation)
        tokens = [word for word in tokens if len(word) > 1]
        # Remove numbers
        tokens = [word for word in tokens if not word.isnumeric()]
        # Remove stopwords
        tokens = [word for word in tokens if word not in all_stopwords]
        tokens2 = []
        for token in tokens:
            if re.search(".*=.*", token) == None and re.search(".*[.-]", token) == None and re.search("[.'-].*", token) == None and re.search(".*[/.:;,_+*~!?^<>{}0-9\\\].*", token) == None :
                tokens2.append(token)

        # Lemmatizing words
        tokens = [lemmatizer.lemmatize(word, wordnet.NOUN) for word in tokens2]
        tokens = [lemmatizer.lemmatize(word, wordnet.VERB) for word in tokens]
        tokens = [lemmatizer.lemmatize(word, wordnet.ADJ) for word in tokens]
        tokens = [lemmatizer.lemmatize(word, wordnet.ADV) for word in tokens]

        #running stop word removal once more after lemmatization
        tokens = [word for word in tokens if len(word) > 1]
        # Remove numbers
        tokens = [word for word in tokens if not word.isnumeric()]
        # Remove stopwords
        tokens = [word for word in tokens if word not in all_stopwords]

        postgreSQL_select_Query5 = "SELECT termid FROM enron.terms WHERE term = %s"
        postgreSQL_insert_Query5 = "INSERT INTO enron.terms(term) VALUES (%s)"
        postgreSQL_select_Query6 = "SELECT * FROM enron.emailterms WHERE emailid = %s and termid = %s"
        postgreSQL_insert_Query6 = "INSERT INTO enron.emailterms VALUES (%s, %s, %s)"
        postgreSQL_update_Query3 = "UPDATE enron.emailterms SET frequency = %s WHERE emailid = %s and termid = %s"

        for token in tokens:
            cursor.execute(postgreSQL_select_Query5, (token,))
            row = cursor.fetchone()
            if row == None:
                cursor.execute(postgreSQL_insert_Query5, (token,))
                connection.commit()  # commiting changes to the database.
            cursor.execute(postgreSQL_select_Query5,(token,))
            record = cursor.fetchone()
            term_ID = record[0] #This is the termid of the term that was just entered into the terms table
            cursor.execute(postgreSQL_select_Query6,(email_ID,term_ID,))
            row = cursor.fetchone()
            if row == None:
                cursor.execute(postgreSQL_insert_Query6,(email_ID,term_ID,1))
                connection.commit()  # commiting changes to the database.
            else:
                cursor.execute("SELECT frequency FROM enron.emailterms WHERE emailid = %s and termid = %s" , (email_ID, term_ID,))
                record = cursor.fetchone()
                updatedFrequency = int(record[0]) + 1
                cursor.execute(postgreSQL_update_Query3,(updatedFrequency, email_ID, term_ID))
                connection.commit()  # commiting changes to the database.


    for directory, subdirectory, filenames in os.walk(rootdir): #os.walk traverses each possible directory in a given directory(here rootdir)
        filenames2 = []
        for filename in filenames:
            filenames2.append(re.sub("_","",filename))
        for filename in sorted(filenames2, key=int): #filenames is a list, so we have to access elements in it one by one
            storeEmail(os.path.join(directory, filename)+"_")


except (Exception, psycopg2.Error) as error:
    print("ERROR IN DB OPERATION:", error)
finally:
    # closing database connection.
    if (connection):
        # Close cursor object and database connection
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")