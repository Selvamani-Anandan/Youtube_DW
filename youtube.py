from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import isodate
from mysql.connector import Error
from sqlalchemy import create_engine, inspect, text
import pandas as pd
import streamlit as st

# creating service connection to youtube
def api_connect():
    api_id="Insert Youtube API Key"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=api_id)

    return youtube

youtube=api_connect()

#Fetch channel info with channel ID
def get_channel_info(channel_id):
    channel_request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    channel_response = channel_request.execute()
    if channel_response["pageInfo"]["totalResults"] > 0:

        channel_data = []
        for item in channel_response['items']:
            data = dict(
                Channel_Name=item["snippet"]["title"],
                Channel_Id=item["id"],
                Subscribers=item['statistics']['subscriberCount'],
                Views=item["statistics"]["viewCount"],
                Total_Videos=item["statistics"]["videoCount"],
                Channel_Description=item["snippet"]["description"],
                Thumbnail = item["snippet"]["thumbnails"]["default"]["url"],
                Playlist_Id=item["contentDetails"]["relatedPlaylists"]["uploads"]
            )
            channel_data.append(data)  # Add the dictionary to the list
    else: 
        return None      

    return channel_data  # Return the list


#Fetch Video IDs with channel ID
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        playlist_items_response=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for item in range(len(playlist_items_response['items'])):
            video_ids.append(playlist_items_response['items'][item]['snippet']['resourceId']['videoId'])
        next_page_token=playlist_items_response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#Fetch Video Info by Video IDs
def get_video_info(video_data):
    video_data_complete = []
    for video_id in video_data:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            # Convert tags list to a comma-separated string if tags exist
            tags = item['snippet'].get('tags', [])
            tags_str = ','.join(tags) if tags else None

            # Convert duration from ISO 8601 to HH:MM:SS
            duration_iso = item['contentDetails']['duration']
            duration = str(isodate.parse_duration(duration_iso))

            data = dict(
                Channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=tags_str,
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description'),
                Published_Date=item['snippet']['publishedAt'],
                Duration=duration,
                Views=item['statistics'].get('viewCount'),
                Likes=item['statistics'].get('likeCount'),
                Comments=item['statistics'].get('commentCount'),
                Favorite_Count=item['statistics']['favoriteCount'],
                Definition=item['contentDetails']['definition'],
                Caption_Status=item['contentDetails']['caption']
            )
            video_data_complete.append(data)
    
    return video_data_complete

#Fetch comment infor
def get_comment_info(video_ids):
    Comment_data = []

    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(
                    Comment_Id=item['snippet']['topLevelComment']['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                Comment_data.append(data)

        except HttpError as e:
            error_reason = e.error_details[0]["reason"]
            if error_reason == "commentsDisabled":
                print(f"Comments are disabled for video ID: {video_id}")
            else:
                print(f"An error occurred for video ID {video_id}: {e}")

    return Comment_data

#Fetch Playlist details
def get_playlist_details(channel_id):
        next_page_token=None
        playlist_details=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        playlist_details.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return playlist_details

#Database connection
def create_connection():
    user = 'root'
    password = 'selvamani'
    host = 'localhost'
    database = 'youtube_dw'
    connection_string = f'mysql+mysqlconnector://{user}:{password}@{host}/{database}'

    engine = create_engine(connection_string)
    return engine

#Complete Channel details
def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    if(ch_details is None):
       return None
    else:
        pl_details = get_playlist_details(channel_id)
        vi_ids = get_videos_ids(channel_id)
        vi_details = get_video_info(vi_ids)
        com_details = get_comment_info(vi_ids)

        # Converting to DataFrames
        df_channel = pd.DataFrame(ch_details)
        df_playlists = pd.DataFrame(pl_details)
        df_videos = pd.DataFrame(vi_details)
        df_comments = pd.DataFrame(com_details)

        engine = create_connection()

        # Inserting each DataFrame into a table
        df_channel.to_sql('channel_table', con=engine, if_exists='append', index=False)
        df_playlists.to_sql('playlist_table', con=engine, if_exists='append', index=False)
        df_videos.to_sql('video_table', con=engine, if_exists='append', index=False)
        df_comments.to_sql('comment_table', con=engine, if_exists='append', index=False)

        return "Saved Channel Details Successfully!"

def show_channels_table():
    engine = create_connection()
    query = "SELECT * FROM channel_table"
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
        df.columns = ["Channel Name", "Channel ID", "Subscribers", "Views","Total Videos", "Channel Description", "Thumbnail link", "Playlist ID"]  
        df_display = df.copy()
        df_display.index = df_display.index + 1
    st_channel_df = st.dataframe(df_display)    
    return st_channel_df

def show_playlists_table():
    engine = create_connection()
    query = "SELECT * FROM playlist_table"
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
        df.columns = ["Playlist ID", "Title", "Channel ID", "Channel Name", "Published At", "Video Count"]  
        df_display = df.copy()
        df_display.index = df_display.index + 1
    st_playlist_df = st.dataframe(df_display)
    return st_playlist_df

def show_videos_table():
    engine = create_connection()
    query = "SELECT * FROM video_table"
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
        df.columns = ["Channel Names", "Channel ID", "Video ID", "Title", "Tags", "Thumbnail", "Description", "Published Date", "Duration", "Views", "Likes", "Comments", "Favourite Count", "Definition", "Caption"]  
        df_display = df.copy()
        df_display.index = df_display.index + 1
    st_video_df = st.dataframe(df_display)
    return st_video_df

def show_comments_table():
    engine = create_connection()
    query = "SELECT * FROM comment_table"
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
        df.columns = ["Comment ID", "Video ID", "Comment Text", "Comment Author", "Comment Published"]  
        df_display = df.copy()
        df_display.index = df_display.index + 1
    st_comments_df = st.dataframe(df_display)
    return st_comments_df
     




#streamlit app UI

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/b/b8/YouTube_Logo_2017.svg", caption=None, width=None, use_column_width=None, clamp=False, channels="RGB", output_format="auto")
    st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    

channel_id=st.text_input("Enter the channel ID", help = "Enter a youtube channel ID to fetch its details and view using below options.", placeholder="Enter a Youtube channel ID and Click Save")

def check_table_exists(engine, table_name):
    """Check if a table exists in the database."""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


engine = create_connection()

if st.button("Save", help = "Fetch and Save Channel ID's  youtube data"):
    if not channel_id:
        st.warning("Please enter a Channel ID")
    elif check_table_exists(engine, "channel_table"):  
        query = text("SELECT COUNT(*) FROM channel_table WHERE Channel_Id = :channel_id")
        engine = create_connection()
        with engine.connect() as connection:
            result = connection.execute(query, {'channel_id': channel_id})
            count = result.scalar()        

        if count > 0:
            st.success("Channel Details of the given channel Id already exists")

        else:
            save_new_channel_info=channel_details(channel_id)
            if save_new_channel_info is None:
                st.warning("Not a Valid Channel ID.")
            else:
                st.success(save_new_channel_info)
    else:
        save_new_channel_info=channel_details(channel_id)
        st.success(save_new_channel_info)
        




if check_table_exists(engine, "channel_table"):
        
    show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"), index = 0)

    if show_table=="CHANNELS":
        show_channels_table()

    elif show_table=="PLAYLISTS":
        show_playlists_table()

    elif show_table=="VIDEOS":
        show_videos_table()

    elif show_table=="COMMENTS":
        show_comments_table()


    question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                                "2. Channels with most number of videos",
                                                "3. 10 most viewed videos",
                                                "4. Comments in each videos",
                                                "5. Videos with higest likes",
                                                "6. Likes of all videos",
                                                "7. Views of each channel",
                                                "8. Videos published in the year of 2022",
                                                "9. Average duration of all videos in each channel",
                                                "10. Videos with highest number of comments"))


    if question=="1. All the videos and the channel name":
        engine = create_connection()
        query = "SELECT title as videos, channel_name as channelname FROM video_table"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df.columns = ["Videos", "Channel Names"]  
            df_display = df.copy()
            df_display.index = df_display.index + 1
        st.write(df_display)

    elif question=="2. Channels with most number of videos":
        engine = create_connection()
        query="SELECT channel_name as channelname,total_videos as no_videos from channel_table order by total_videos desc"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df.columns = ["Channel Name", "Number of videos"]  
            df_display = df.copy()
            df_display.index = df_display.index + 1
        st.write(df_display)

    elif question=="3. 10 most viewed videos":
        engine = create_connection()
        query="SELECT views as views,channel_name as channelname,title as videotitle from video_table where views is not null order by views desc limit 10"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df.columns = ["Views", "Channel Name", "Video Title"] 
            df_display = df.copy()
            df_display.index = df_display.index + 1
        st.write(df_display) 

    elif question=="4. Comments in each videos":
        engine = create_connection()
        query="SELECT comments as no_comments,title as videotitle from video_table where comments is not null"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df.columns = ["Number of comments", "Video Title"]
            df_display = df.copy()
            df_display.index = df_display.index + 1
        st.write(df_display)

    elif question=="5. Videos with higest likes":
        engine = create_connection()
        query="SELECT title as videotitle,channel_name as channelname,likes as likecount from video_table where likes is not null order by likes desc"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df.columns = ["Video Title", "Channel Name", "Likes Count"]  
            df_display = df.copy()
            df_display.index = df_display.index + 1
        st.write(df_display)


    elif question=="6. Likes of all videos":
        engine = create_connection()
        query="SELECT likes as likecount,title as videotitle from video_table"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df.columns = [ "Likes Count", "Video Title"] 
            df_display = df.copy()
            df_display.index = df_display.index + 1
        st.write(df_display)

    elif question=="7. Views of each channel":
        engine = create_connection()
        query="SELECT channel_name as channelname ,views as totalviews from channel_table"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df.columns = [ "Channel Name", "Total Views"]
            df_display = df.copy()
            df_display.index = df_display.index + 1
        st.write(df_display)

    elif question=="8. Videos published in the year of 2022":
        engine = create_connection()
        query="select title as video_title,published_date as videorelease,channel_name as channelname from video_table where extract(year from published_date)=2022"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df.columns = [ "Video Title", "Video Release", "Channel Name"] 
            df_display = df.copy()
            df_display.index = df_display.index + 1
        st.write(df_display)

    elif question=="9. Average duration of all videos in each channel":
        engine = create_connection()
        query="SELECT channel_name as channelname,AVG(TIME_TO_SEC(duration)) / 3600 AS average_duration_in_hours from video_table group by channel_name"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df.columns = ["Channel Name", "Average Duration"]  
            df_display = df.copy()
            df_display.index = df_display.index + 1
        st.write(df_display)

    elif question=="10. Videos with highest number of comments":
        engine = create_connection()
        query="select title as videotitle, channel_name as channelname,comments as comments from video_table where comments is not null order by comments desc"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df.columns = [ "Video Title", "Channel Name", "Comments"]  
            df_display = df.copy()
            df_display.index = df_display.index + 1
        st.write(df_display)

 
