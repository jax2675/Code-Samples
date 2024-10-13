# Setup
# Importing required libraries
import requests, json, pymongo, pprint, os, datetime, sqlite3
from decimal import Decimal

# Uses requests library to scrape file from website
# Args: url: string Must be url for json file
# Returns list of json objects from json file
def scraper(url):
    # URL request to retrieve data
    fileData = requests.get(url)
    # Checks for successful status_code
    if fileData.status_code == 200:
        # deserializes data
        jData = fileData.json()
        # returns list of json objects
        print(f"Retrieved {len(jData)} documents from {url}") 
        return jData
    else:
        # prints error message with status code if url request is
        # not successful and returns None for error hadling
        print(f"Data retrieval error: {fileData.status_code}")
        return None

# Writes data to json file
# Args: data: list of json objects, fileName string json file name to write to
# Returns  True if write is successful, None if not for error handling
def toJsonFile(data, fileName):
    # Error handling
    try:
        # opens output file and creates file object f
        with open(fileName, 'w') as f:
            # writes JSON formated data to output file
            # indents JSON objects
            f.write(json.dumps(data, indent=4))
        # closes file
        f.close()
        # returns True for success
        return True
    except Exception:
        # Prints error statement and returns None for failure
        print("Error writing to JSON file.")
        return None

# Retrieves the user name and password from local file
# Creates a uri string with the data pulled from file
# To obfuscate login information
# Returns string uri for MongoDb connection
def getMongoURI(credFile):
    # checks that credFile is valid file
    if os.path.exists(credFile) :
        # Opens credential file
        with open(credFile, 'r') as f:
            # reads password and user id from file
            # strips newline character and whitespace from lines
            pw = f.readline().strip()
            uid = f.readline().strip()
        # close file and return uri string
        f.close()
        return f"mongodb+srv://{uid}:{pw}@ucd6020.ccan0.mongodb.net/?retryWrites=true&w=majority&appName=UCD6020"
    else :
        # Prints error statement and returns None for failure
        print(f"File \'{credFile}\' does not exist in current filepath.")
        return None


# Connect to the MongoDB database
# Args: uri =string
# Returns: pymongo client obj
def connectMongoDB(uri):
    # Connects to MongoDB client
    client = pymongo.MongoClient(uri)
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged MongoDB deployment. Connection successful.")
    except Exception:
        # Prints error statement and returns none if connection fails
        print("MongoDB connection Failed")
        return None
    # returns client if successful
    return client

# adds data to the MongoDB
# Args : data: list of json objects, db: pymongo.database.Database
# Returns: pymongo.results.InsertManyResult
def mdbInsert(data, db):
    # drops collection if it is in database
    if db.meteorite_landings.count_documents({}) > 0:
        # deletes collection for testing
        db.meteorite_landings.drop()

    # inserts all of the JSON data into the database
    result = db.meteorite_landings.insert_many(data)
    # error handling: returns result if result set is not empty
    if result:
        print(f"Inserted {db.meteorite_landings.count_documents({})} into meteorite_landings collection.")
        return result
    # prints error message and returns None if fails
    else:
        print("Data did not load to MongoDB or dataset is empty.")
        return None# Uses requests library to scrape file from website

# Extracts documents from MongoDB
# Args : coll: pymongo Collection, f: str query filter
#        proj: str query projections
# Returns an array of dictionaries if successful, None if fail
def extractMDB(coll, f, proj):
    # Runs a find query with the above filter and projections
    listings = coll.find(f, proj)

    meteoriteList = []
    # iterates through query results and assigns them to a dictionary
    # the new dictionary is appended to the data array
    for rec in listings:
        # Convert price (Decimal128) to float and handle missing fields
        dateTemp = getMonthYear(rec.get("year", ""))
        meteoriteList.append({
            # Inserts empty string if no value present
            "name": rec.get("name", ""),
            # id must exist
            "id": int(rec["id"]),
            # Inserts 0.0 if does no value present
            "mass": float(rec["mass"]) if "mass" in rec else 0.0,
            "year": dateTemp[0],
            "reclat": float(rec["reclat"]) if "reclat" in rec else 0.0,
            "reclong": float(rec["reclong"]) if "reclong" in rec else 0.0,
        })
    if len(meteoriteList) > 0:
        # Returns array of dictionaries if records found
        print(f"Retrieved {len(meteoriteList)} documents from NobgoDB.")
        return meteoriteList
    else :
        # returns None if no records found
        return None

# Splits the date string and retrieves month and year
# Args : dateStr: str datetime must be separated by '-'
# Returns: array: index 0 year and 1 month, [0,0] if no date
def getMonthYear(dateStr) :
    if dateStr != "" :
        spltStr = dateStr.split('-')
        return [int(spltStr[0]), int(spltStr[1])]
    else :
        return [0,0]


# Opens connection to SQLite database 
# Create table for with parameters provided
# Args : dbName = str SQLite DB Name, 
#        tableName = str Table name , 
#        keys = str Table attribute names and options
# Returns True if successful, None if fails
def createSqliteDB(dbName, tableName, keys):
    # Creates a SQLiite DB at the filepath specified in dbName
    conn = sqlite3.connect(dbName)
    # Gets a cursor obj for SQLite DB
    cursor = conn.cursor()
    # Drops the table specified in tableName if it exists for
    # testing purposes
    cursor.execute(f"DROP TABLE IF EXISTS {tableName}")
    # Creates table specified in tableName and in keys
    # if it doesn't exist
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {tableName} (
            {keys})''')
    # commits changes and closes connection to SQLite DB
    conn.commit()
    # gets table information for verification purposes
    check = cursor.execute(f"PRAGMA table_info({tableName})")
    r = check.fetchone()
    # If table exists close connection and return true for success
    if r is not None :
        conn.close()
        return True
    # Close connection and return false for failure
    else :
        conn.close()
        return None

# Insert the extracted data into the SQLite database
# Args: curs = cursor to SQLite DB, tableName = String
#       data = array of dictionary items
def insertSqlite(cursor, tableName, data):
    # iterates through the dictionaries(documents) in the array
    for rec in data:

        # Inserts the document into the table if it isn't
        # already there
        cursor.execute(f'''
            INSERT OR IGNORE INTO {tableName}
            (id, name, mass, year, reclat, reclong)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            int(rec['id']), rec['name'], rec['mass'],
            rec['year'], rec['reclat'], rec['reclong']
        ))

def testAndRun():

    fileInfo = scraper("https://data.nasa.gov/resource/y77d-th95.json")
    assert not fileInfo == None, "getDataFile failed"
    assert isinstance(fileInfo, list), "getDataFile failed"

    fileCheck = toJsonFile(fileInfo, "projectText.json")
    assert not fileCheck == None
    assert fileCheck

    muri = getMongoURI("pwProj.txt")
    # test getMongoURI()
    assert muri is not None, "Geting URI failed."
    assert len(muri) > 0, "Getting URI failed."

    client = connectMongoDB(muri)
    assert not client == None
    assert isinstance(client, pymongo.mongo_client.MongoClient)

    db = client.meteorite_data
    results = mdbInsert(fileInfo, db)
    assert not results == None
    assert isinstance(results, pymongo.results.InsertManyResult)

    # Verification that data was added to MongoDB
    # prints documents in dataset
    print(f"There are {len(fileInfo)} entries in the dataset")
    # prints documents in collection
    print(f"{db.meteorite_landings.count_documents(
        {})} documents were added to the MongoDB collection meteorite_landings")
    # prints first document in collection
    print("Document example:")
    pprint.pprint(db.meteorite_landings.find_one({}), indent=4)

    # Create filter and projections for MongoDB query
    # Retrieves documents where the mass field exists and fall field is not "Found"
    filtr = {"mass": {"$exists": True}, "fall": {"$ne": "Found"}}
    # Does not return _id field, returns all fields marked 1
    proj = {"_id": 0, "id": 1, "name": 1, "mass": 1,
            "year": 1, "reclat": 1, "reclong": 1}

    # Extracts the required data for analysis from the meteorite_landings
    # MongoDB collection
    meteoriteList = extractMDB(db.meteorite_landings, filtr, proj)
    # Verification that data was retrieved from MongoDB
    print(f"Number of documents returned from MongoDB query: {
          len(meteoriteList)}")
    # Testing extractMeteorite function
    assert isinstance(meteoriteList, list)
    assert not meteoriteList == []
    assert meteoriteList is not None
    assert isinstance(meteoriteList[0], dict)
    # Validating that indeividual data values are the correct type
    for rec in meteoriteList:
        assert isinstance(rec["id"], int)
        assert isinstance(rec["name"], str)
        assert isinstance(rec["mass"], float)
        assert isinstance(rec["year"], int)
        assert isinstance(rec["reclat"], float)
        assert isinstance(rec["reclong"], float)

    sqliteDB = "MeteoriteData.sqlite"
    tableName = "meteorite_landings"
    keys = '''id INTEGER PRIMARY KEY UNIQUE NOT NULL, name TEXT NOT NULL,
    mass REAL NOT NULL, year INTEGER NOT NULL,
    reclat REAL NOT NULL, reclong REAL NOT NULL'''

    try:
        # Create SQLite DB for listings retrieved
        sqlCreateRes = createSqliteDB(sqliteDB, tableName, keys)
        assert sqlCreateRes is not None
        assert sqlCreateRes
        print("SQLite DB table created successfully.")
        # connect to the new SQLite DB
        conn = sqlite3.connect(sqliteDB)
        # Create a cursor object to manipulate DB with
        cursor = conn.cursor()
        # Insert data into the SQLite DB
        insertSqlite(cursor, tableName, meteoriteList)
        print("SQLite insert complete.")
        # Commit changes to DB and close connection
        conn.commit()
        # Getting number of rows
        count = cursor.execute(f"SELECT COUNT(*) FROM {tableName}")
        val = count.fetchone()
        conn.close()
        print(f"{val[0]} records added to SQLite table successfully.")        
    except Exception as e:
        print(f"Error: {e}")
        print("Failure with SQLite DB")


testAndRun()
