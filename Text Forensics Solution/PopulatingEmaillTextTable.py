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

    rootdir = "C:\\Users\\user\Desktop\mydata"
    my_parser = Parser()  # Used to parse the email files

    def storeEmailTextInDB(inputfile):
        with open(inputfile, "r") as file:  # Like this the file gets closed automatically
            data = file.read()  # getting the raw email text

        email = my_parser.parsestr(data)  # parsing the text
        email_body = str(email.get_payload())

        postgres_insert_query1 = "INSERT INTO enron.emailfulltext(emailtext) VALUES (%s)"
        cursor.execute(postgres_insert_query1, (email_body,))
        connection.commit()  # commiting changes to the database


    for directory, subdirectory, filenames in os.walk(rootdir):  # os.walk traverses each possible directory in a given directory(here rootdir)
        filenames2 = []
        for filename in filenames:
            filenames2.append(re.sub("_", "", filename))
        for filename in sorted(filenames2, key=int):  # filenames is a list, so we have to access elements in it one by one
            storeEmailTextInDB(os.path.join(directory, filename) + "_")

except (Exception, psycopg2.Error) as error:
    print("ERROR IN DB OPERATION:", error)
finally:
    # closing database connection.
    if (connection):
        # Close cursor object and database connection
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")