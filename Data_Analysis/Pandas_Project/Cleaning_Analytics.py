# Importing required libraries
import pymongo, sqlite3, datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display, HTML

# Opens connection to SQLite database 
# Args : dbName = str SQLite DB Name, 
# Returns: sqlite3.Connection object if successful, 
#          None if fails
def connectSqlite(dbName):
    try :
        # Creates a SQLiite DB at the filepath specified in dbName
        conn = sqlite3.connect(dbName)
        return conn
    except Exception as e :
        print(f"Error connecting to {dbName}")
        return None

# Extract data from SQLite table and loads into pd.DataFrame
# Args: conn: sqlite3.Connection obj, tableName: str: name of SQLite table
# Returns: pd.DataFrame obj, if successfule, None if extraxction fails
def sqlToDataframe(conn, tableName) :
    
    try :
        # read sqlite3 data from tableName table, assigns to dataframe obj 
        meteoriteDF = pd.read_sql(f"SELECT * FROM {tableName}", conn, index_col="id",  dtype={'year': 'Int64'})
        # Displays first 3 rows of DataFrame as verification
        # Create title using HTML
        title = '<h3>First 3 Rows of Data</h3>'

        # Display the title and the DataFrame
        display(HTML(title))
        display(meteoriteDF.head(3))
        # returns dataframe if successful
        return meteoriteDF
    except Exception as e :
        # Print error message and return None if fails
        print(f"Error retrieving data from SQLite table {tableName}.")
        print(f"Error: {e}")        
        return None

# SQLite Database Extraction
try:
    # SQL database and table name
    sqliteDB = "MeteoriteData.sqlite"
    tableNameSQL = "meteorite_landings"
    
    # Connect to SQLite DB 
    conn = connectSqlite(sqliteDB)
    assert conn is not None
    assert isinstance(conn, sqlite3.Connection)
    
    # Extract from sqlite db, load to df
    df = sqlToDataframe(conn, tableNameSQL)
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    conn.close()
    print(f"{len(df.index)} records extracted from SQLite table (tableNameSQL) successfully.")
except Exception as e:
    conn.close()
    print(f"Error: {e}")
    print("Failure with SQLite DB")


# Get the 3 largest meteorite landings by mass, before cleaning up the data and removing outliers
# title for output
title = "<h3>3 Largest Meteorite Landings by Mass</h3>"
biggestLandings = df.sort_values(by='mass', ascending=False).head(3)
display(HTML(title))
display(biggestLandings)

# Checks for and deals with missing values 
# In the event of needing to remove some missing vals and replace others
# replace values first, then delete the remaining ones
# Args: df: Dataframe obj, delete: Boolean delete all missing vals if True
# Returns: True: missing values found and replaced or deleted, 
#          False: no missing values found 
def missingVal(df, delete=False) :
    # sums the missing values for each column
    missingCount = df.isnull().sum()
    # Displays series showing where missing values are
    print("Missing Values: \n", missingCount)
    # Checks if any missing values were found
    if missingCount.any() :       
        if delete :
            # Drops all missing values if delete == True
            df.dropna(inplace=True)
        else :
            # Iterates through missingCount series, 
            # gets value and index for each entry
            for col in df.columns :
                if pd.api.types.is_numeric_dtype(df[col]) :
                    # Replaces missing value with the mean value for the column
                    # if a value is missing and the column is numeric
                    df[col].fillna(df[col].mean(), inplace=True)
        # Verification that missing values have been deleted/replaced
        print("Missing Values After: \n", df.null().sum())
        return True
    else :
        # No missing values
          return False

# Looks for duplicate values and removes them if found
# Args: df: Dataframe
# Returns: True: If duplicates found, False: If no duplicates
def duplicateVals(df) :
    # gets duplicate rows
    duplicates = df[df.duplicated(keep=False)]
    # Checks if duplicates were found
    if not duplicates.empty:
        # print statement for verification
        print("Duplicates found: \n", duplicates)
        # deletes duplicate rows
        df.drop_duplicates(inplace=True)
        print("Duplicates dropped")
        # Returns true, duplicates were found
        return True
    # No duplicates, returns false
    print("No duplicates found.")
    return False

# Looks for values that are outside of a given range (inclusive) and 
# replaces/deletes them; Replaces values with either rangeMin (below minimmum) 
# or rangeMax (Above maximum)
# Args: df: DataFrame obj, rangeMin: minimum value for acceptible range,
#       rangeMax: Maximum value for acceptible range, 
#       delete: Boolean: True: delete values, False: replace values
# Returns: DataFrame: If out-of-bounds values found and replaced/removed
#          False: If no out-of-bound values are found
def outOfRange(df, column, rangeMin, rangeMax, delete=False) :
    # Error Handling if duplicates are present
    try : 
        # Store original length to know how many rows were deleted
        origLength = len(df)
        if delete :
            # Gets rows where column values are within range given replaces df
            # with dataframe of only those values
            df = df[(df[column] >= rangeMin) & (df[column] <= rangeMax)]
            # Calculates how many rows deleted
            print(f"Deleted {origLength - len(df)} rows from column '{column}' outside range [{rangeMin}, {rangeMax}]")
        else:
            # gets the number of out of range values to be replaced
            count = df[(df[column] < rangeMin) | (df[column] > rangeMax)].shape[0]
            # replaces the values that are outside the lower and upper limits with those values.
            df[column] = df[column].clip(lower=rangeMin, upper=rangeMax)
            # Print statement for verification
            print(f"Replaced {count} out-of-range values in column '{column}' with range [{rangeMin}, {rangeMax}]")
        return df
    except Exception as e :
        # print error statements and returns false
        print("Error checking out-of-bounds values in DataFrame. Check for duplicates first.")
        print(f"Error: {e}")
        return False
        
# Method to drive cleaning data
# Args: df: DataFrame
# Returns: temp: DataFrame of cleaned data
def scrub_a_dub(df) :
    # Create title using HTML
    title = '<h3>Cleaning data...</h3>'

    # Display the title and the DataFrame
    display(HTML(title))
    # min and max range values
    bounds = {
        "reclong" : (-180.000, 180.000),
        "reclat" : (-90.000, 90.000),
        "year" : (861, 2016), 
        "mass" : (0, 750000)}
    
    # Check for and handle missing values
    missingVal(df, False)
    
    # Checks for and handles duplicates
    duplicateVals(df)
    
    # Checks for and handles values that are out of range
    for col, (min, max) in bounds.items() :
        # calls out of range for each item in bounds, delete = true if column is 'year'
        df = outOfRange(df, col, min, max, delete=(col=="year")) 
    return df


# Create title using HTML
title = '<h3>Maximum Values</h3>'
# Display the title and the DataFrame
display(HTML(title))
display(df.max(numeric_only=True))
# Create title using HTML
title = '<h3>Minimum Values</h3>'

# Display the title and the DataFrame
display(HTML(title))
display(df.min(numeric_only=True))

# Clean the data
df = scrub_a_dub(df)
assert df is not None
assert df is not False
assert isinstance(df, pd.DataFrame)

# Generate timestamped filename for version control
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"meteorite_clean_{timestamp}.csv"

# saving cleaned data to CSV file
df.to_csv(filename)

# Verifying data written to CSV file
# read from csv file, explicitly assigning data types to keep different types from returning false on comparison 
dfVerify = pd.read_csv(filename, index_col='id', dtype={'id': 'int64','mass': float, 'year': 'Int64', 'reclat': float, 'reclong': float, 'name': str})
# asserting that dataframe written to csv and dataframe read from csv are equal
assert df.equals(dfVerify)

# Perform data analysis
# Create title using HTML
title = '<h3>Standard Deviation</h3>'

# Display the title and the DataFrame
display(HTML(title))
display(df.std(numeric_only=True))
## mass has a wide distribution, could sanitize the data by standardizing it
## month looks to be a rather useless variable, it could be dropped from the dataset before further analysis
## year, reclat and reclong look to have a good distribution in terms of std deviation

# Create title using HTML
title = '<h3>Skewness Values</h3>'

# Display the title and the DataFrame
display(HTML(title))
display(df.skew(numeric_only=True))

# Create title using HTML
title = '<h3>Summary Statistics</h3>'

# Display the title and the DataFrame
display(HTML(title))
display(df.describe())

# Create title using HTML
title = '<h3>Average Mass by Year</h3>'

# Display the title and the DataFrame
display(HTML(title))
avgMassByYear = df.groupby("year")["mass"].mean()
display(avgMassByYear)

# Create title using HTML
title = '<h3>Average Mass by Latitude</h3>'

# Display the title and the DataFrame
display(HTML(title))
avgMassByLat = df.groupby("reclat")["mass"].mean()
display(avgMassByLat)

# Create title using HTML
title = '<h3>Average Mass by Longitude</h3>'

# Display the title and the DataFrame
display(HTML(title))
avgMassByLong = df.groupby("reclong")["mass"].mean()
display(avgMassByLong)
## There might be a pattern with avg by lat, larger hits closer to equator?

# Create title using HTML
title = '<h3>Correlation Matrix</h3>'

# Display the title and the DataFrame
display(HTML(title))
corr = df.corr(numeric_only=True)
display(corr)

# Visualizations
# Create title using HTML
title = '<h2>Visualizations</h2>'
# Display the title
display(HTML(title))

# HTML title
title = '<h3>Histograms for Each Numeric Variable</h3>'
# Display the title and the DataFrame
display(HTML(title))

# creates a 2 by 2 display for plots
fig, axs = plt.subplots(2,2)

# creates a histogram for each numeric variable and assigns it to a subplot sector
axs[0,0].hist(df['year'], bins=12, range=(df['year'].min(), df['year'].max()), color='blue')
axs[0,0].set_title('year')
axs[0,1].hist(df['mass'], bins=12, range=(df['mass'].min(), df['mass'].max()), color='green')
axs[0,1].set_title('mass')
axs[1,0].hist(df['reclat'], bins=12, range=(df['reclat'].min(), df['reclat'].max()), color='purple')
axs[1,0].set_title('reclat')
axs[1,1].hist(df['reclong'], bins=12, range=(df['reclong'].min(), df['reclong'].max()), color='orange')
axs[1,1].set_title('reclong')
plt.tight_layout()
plt.savefig("project/histograms.png", bbox_inches='tight')
plt.show()
plt.close('all')

# set size to keep labels from overlapping
plt.figure(figsize=(3, 3))
# create color matrix from correlation table
plt.pcolormesh(corr)
# set x and y axis labels
plt.xticks(range(4), corr.index, rotation=45)
plt.yticks(range(4), corr.index)
# inverts plot, values of 1 start at top left
plt.ylim(4,0)
# adds color bar
plt.colorbar()
# adds title
plt.title("Meteorite Landing Correlation Matrix", loc='center')
# saves matrix to file, bbox_inches keeps labels from being cut off
plt.savefig("project/corrColorMatrix.png", bbox_inches='tight')
plt.tight_layout()
plt.show()
plt.close('all')

# HTML title
title = '<h3>Scatter plots</h3>'
# Display the title and the DataFrame
display(HTML(title))

# Scatter plot Mass vs Year
figScatter = df.plot.scatter(x='mass', y='year', xlabel="Mass", ylabel="Year", title="Mass by Year", logx=False)
figScatter.figure.savefig("project/scatterMassYear.png", bbox_inches='tight')
plt.show()
plt.close('all')

# Scatter plot Latitude vs Year
figScatter1 = df.plot.scatter(x='reclat', y='year', xlabel="Latitude logx", ylabel="Year", title="Latitude by Year", logx=True)
figScatter1.figure.savefig("project/scatterLatYear.png", bbox_inches='tight')
plt.show()
plt.close('all')

# Geospatial mapping of meteorite landings
# x and y axies are longitude and latitude
# A 3rd dimension (color) is added to account for the mass of the meteorite. 
plt.scatter(df['reclong'], df['reclat'], c=df['mass'], cmap='viridis', alpha=0.5)
# include color bar to show what the colors mean
plt.colorbar(label='Mass of Meteorite')
plt.title('Meteorite Landings by Location')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.savefig("project/geospatialMap.png")
plt.show()
plt.close('all')

# getting number of meteorite landings per year for the most current 400 years
freqSeries= df[(df['year'] >= 1616) & (df['year'] <= 2016)].groupby('year').size()
# Calculating line of best fit using numpy polyfit and covariance matrix
coefficients, cov = np.polyfit(freqSeries.index, freqSeries.values, 1, full=False, cov=True) # 1 = linear regression
# creates 1 dimensional polynomial for plotting the line
fit = np.poly1d(coefficients)

# Sets display size
plt.figure(figsize=(4, 3))
# plots number of meteorite landings per year using accessible color pairing
plt.plot(freqSeries.index, freqSeries.values, 'o',color='#05fe04')
# plots the line of best fit using accessible color pairing
plt.plot(freqSeries.index, fit(freqSeries.index), linestyle='-', color='#d71b60')
# Creates labels for plot
plt.title('Number of Landings per Year with Line of Best Fit')
plt.xlabel('Year')
plt.ylabel('Number of Meteorite Landings')
# saves plot to file
plt.savefig('project/landingsPerYearLine.png')
plt.show()
plt.close('all')
