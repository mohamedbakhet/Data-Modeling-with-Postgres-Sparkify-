import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime

def extract(ts):
    """
    Description: This function is responsible for extract informations about time like hour, day, month and etc from timestamp.
    Arguments:
        ts: timestamp.

    Returns:
        list of details about time like hour, day, month and etc.  
    """
    timwst=[]
    hour_ls=[]
    day_ls=[]
    week_ls=[]
    month_ls=[]
    year_ls=[]
    week_d_ls=[]
    for i in ts:
        timwst.append(i)
        hour_ls.append(i.strftime("%H"))
        day_ls.append(i.strftime("%d"))
        week_ls.append(i.strftime("%U"))
        month_ls.append(i.strftime("%m"))
        year_ls.append(i.strftime("%Y"))
        week_d_ls.append(i.strftime("%A"))
    return  [timwst,hour_ls,day_ls,week_ls,month_ls,year_ls,week_d_ls]
def convert(x):
    """
    Description: This function is responsible for convert the timestamp with milliseconds to datetime format.
    Arguments:
        x: timestamp with milliseconds .

    Returns:
        timestamp with datetime format.  
    """
    return datetime.datetime.fromtimestamp(x/1000.0)
def process_song_file(cur, filepath):
    """
    Description: This function is responsible for extract data from json song data file and apply some etl and insert the data in database on songs and artists tables.
    Arguments:
        cur: the cursor object.
        filepath: song data file path.
    Returns:
        None
    """
    # open song file
    df =pd.read_json(filepath, lines=True)

    # insert song record
    song_data = (df[['song_id','title','artist_id','year','duration']].values)[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = (df[['artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude']].values)[0].tolist()

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function is responsible for extract data from json log data file and apply some etl and insert the data in database on user,songplay and time tables.
    Arguments:
        cur: the cursor object.
        filepath: log data file path.
    Returns:
        None
    """
    # open log file
    df =  pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df['ts']= df['ts'].apply(convert)
    t=df.copy()
    
    
    # insert time data records
   
    time_data = extract(t['ts'])
    column_labels = ("timestamp","hour", "day", "week of year","month", "year","weekday")
    dict_time={}
    dict_time[column_labels[0]]=time_data[0]
    dict_time[column_labels[1]]=time_data[1]
    dict_time[column_labels[2]]=time_data[2]
    dict_time[column_labels[3]]=time_data[3]
    dict_time[column_labels[4]]=time_data[4]
    dict_time[column_labels[5]]=time_data[5]
    dict_time[column_labels[6]]=time_data[6]
    time_df = pd.DataFrame(dict_time) 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    user_df = user_df.drop_duplicates()[user_df.drop_duplicates()!=""]


    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row[['ts','userId','level']].values).tolist()+[songid]+ [artistid]+(row[['sessionId','location','userAgent']].values).tolist()
 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()