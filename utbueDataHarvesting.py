import googleapiclient.discovery
from pprint import pprint
import pymongo
import pandas as pd
import sqlite3
import streamlit as st

# API key to get data from youtube data api
api_key = 'AIzaSyAIsMVvYHEHPZmKQ0osFJcLPwHCvT25jfA'
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# mongo db connection
connObj = pymongo.MongoClient("mongodb://Godwin:Qwertyuiop@ac-btwmlnw-shard-00-00.jjlhwxd.mongodb.net:27017,ac-btwmlnw-shard-00-01.jjlhwxd.mongodb.net:27017,ac-btwmlnw-shard-00-02.jjlhwxd.mongodb.net:27017/?ssl=true&replicaSet=atlas-icqazk-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
dbName = connObj['youtubeDB'] #creating a database in mongodb

# creating a database in sqlite3
conn = sqlite3.connect('youtube.db')
cursor = conn.cursor() #creating a cursor to execute a queries


def getChannelDetails(channelID):
    # get channel details
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channelID
        )
    # "UCX6OQ3DkcsbYNE6H8uQQuVA"

    response = request.execute()
    videodetailslist = []
    videodetailslist.append({response['items'][0]['snippet']['title']: {
        'logo': response['items'][0]['snippet']['thumbnails']['medium']['url'],
        'title': response['items'][0]['snippet']['title'],
        'channelID': response['items'][0]['id'],
        'description': response['items'][0]['snippet']['description'],
        'startDate': response['items'][0]['snippet']['publishedAt'],
        'totalSubscribers': response['items'][0]['statistics']['subscriberCount'],
        'totalVideos': response['items'][0]['statistics']['videoCount'],
        'totalViews': response['items'][0]['statistics']['viewCount'],
        'playlistID': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }})
    nextpagetoken = None
    videoidslist = []
    tempcount = 0
    while True:
        # get videos ids
        playlistrequest = youtube.playlistItems().list(
            part = "snippet",
            playlistId = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            maxResults = 50,
            pageToken = nextpagetoken
        ).execute()
        for i in playlistrequest['items']:
            # get videos list
            videosrequest = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=i['snippet']['resourceId']['videoId']
            ).execute()
            # appending the received videos details to a list
                
            for indexkey, y in enumerate(videosrequest['items']):
                tempcount = int(tempcount)+1
                comments = {}
                try:
                    # geting comments details
                    commentsrequest = youtube.commentThreads().list(
                    part="snippet",
                    videoId=i['snippet']['resourceId']['videoId'],
                    maxResults = 100
                    ).execute()
                    # adding comments details to the appropriate videos
                    for key, value in enumerate(commentsrequest['items']):
                        commentincremented = key+1
                        comments['comment'+str(commentincremented)] = {
                                'commentID': value['id'],
                                'videoId': value['snippet']['videoId'],
                                'channelId': value['snippet']['channelId'],
                                'comment': value['snippet']['topLevelComment']['snippet']['textDisplay'],
                                'commentedUserName': value['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                'commentpublishedAt': value['snippet']['topLevelComment']['snippet']['publishedAt']
                            }
                except:
                    pass
                videodetailslist.append({'video'+str(tempcount): {
                    'channelID': y['snippet']['channelId'],
                    'title': y['snippet']['title'],
                    'thumbnails': y['snippet']['thumbnails']['default']['url'],
                    'tags': y['snippet'].get('tags'),
                    'publishedAt': y['snippet']['publishedAt'],
                    'videoID': y['id'],
                    'totalLikes': y['statistics'].get('likeCount'),
                    'totalViews': y['statistics'].get('viewCount'),
                    'totalComments': y['statistics'].get('commentCount'),
                    'favoriteCount': y['statistics'].get('favoriteCount'),
                    'duration': y['contentDetails'].get('duration'),
                    'caption': y['contentDetails'].get('caption'),
                    'comments': comments
                }})
                videoidslist.append(i['snippet']['resourceId']['videoId'])
        nextpagetoken = playlistrequest.get('nextPageToken')
        if nextpagetoken is None:
            break
    return {'videodetailslist': videodetailslist, 'title': response['items'][0]['snippet']['title']}


def insertToMongoDB(getChannelDetailsResponse):
    channelName = ''
    for respvalue in getChannelDetailsResponse:
        namekey = list(respvalue.keys())
        channelName = namekey[0]
        break
    dbCollectionName = dbName[channelName]
    dbCollectionName.drop()
    dbCollectionName = dbName[channelName]
    dbCollectionName.insert_many(getChannelDetailsResponse)
    channelList = []
    for data in dbCollectionName.find({},{'_id':0}):
        channelList.append(data)
        break
    return {'channelList': channelList, 'dbCollectionName': dbCollectionName}


def channelTableCreation():
    createchanneltable = '''create table if not exists
    channelDetail(channelID varchar(255) primary key, title varchar(255), totalViews bigint, description text, totalSubscribers bigint, 
    totalVideos bigint, playlistID varchar(255), logo varchar(255), startDate varchar(255))'''
    cursor.execute(createchanneltable)



def inserttochanneltable(insertingData):
    val = list(insertingData[0].values())
    dfDcit = {}
    for key, value in enumerate(val[0]):
        dfDcit[value] = [val[0][value]]
    channeldetailsdf = pd.DataFrame(data=dfDcit)
    for index, rows in channeldetailsdf.iterrows():
        values = (rows['channelID'],
        rows['title'],
        rows['totalViews'],
        rows['description'],
        rows['totalSubscribers'],
        rows['totalVideos'],
        rows['playlistID'],
        rows['logo'],
        rows['startDate'])
        
        cursor.execute("SELECT * FROM channelDetail WHERE channelID=?", (rows['channelID'],))
        existingrecord = cursor.fetchone()
        if not existingrecord:
            insertQuery = '''insert into channelDetail(channelID, title, totalViews, description, 
            totalSubscribers, totalVideos, 
            playlistID, logo, startDate)
            values(?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            cursor.execute(insertQuery, values)
        else:
            pass
    



def videosTableCreation():
    createvideostable = '''create table if not exists
    videosDetail(videoID varchar(255) primary key, title varchar(255), totalViews bigint, thumbnails text, 
    totalLikes bigint, 
    totalComments bigint, channelID varchar(255), duration interval, publishedAt varchar(255), 
    foreign key (channelID) references channelDetail (channelID))'''
    cursor.execute(createvideostable)



def inserttovideostable(dbCollectionName, title):
    for x in dbCollectionName.find({},{'_id':0, title:0}):
        tempvideolist = list(x.values())
        tempdict = {}
        if len(tempvideolist)!=0:
            for y in tempvideolist:
                for key, val in enumerate(y):
                    tempdict[val] = [tempvideolist[0][val]]
        videodetailsdf = pd.DataFrame(data=tempdict)
        for index, rows in videodetailsdf.iterrows():
            values = (rows['videoID'],
            rows['title'],
            rows['totalViews'],
            rows['thumbnails'],
            rows['totalLikes'],
            rows['totalComments'],
            rows['channelID'],
            rows['duration'],
            rows['publishedAt'])

            cursor.execute("SELECT * FROM videosDetail WHERE videoID=?", (rows['videoID'],))
            existingrecord = cursor.fetchone()
            if not existingrecord:
                insertQuery = '''insert into videosDetail(videoID, title, totalViews, thumbnails, 
                totalLikes, totalComments, 
                channelID, duration, publishedAt)
                values(?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                cursor.execute(insertQuery, values)
            else:
                pass



def commentsTableCreation():
    createcommentstable = '''create table if not exists
    commentsDetail(commentID varchar(255) primary key, comment varchar(255), commentedUserName text,  
    videoId varchar(255), channelId varchar(255), commentpublishedAt varchar(255), 
    foreign key (videoId) references videosDetail (videoId))'''
    cursor.execute(createcommentstable)



def inserttocommentstable(dbCollectionName, title):
    for x in dbCollectionName.find({},{'_id':0, title:0}):
        tempvideolist = list(x.values())
        tempdict = {}
        if len(tempvideolist)!=0:
            temparr = [tempvideolist[0]['comments']]
            for y in temparr:
                if y!={}:
                    for key, val in enumerate(y):
                        for index, dictval in enumerate(y[val]):
                            tempdict[dictval] = [y[val][dictval]]          
            
                        commentdetailsdf = pd.DataFrame(data=tempdict)
                        for index, rows in commentdetailsdf.iterrows():
                            values = (rows['commentID'],
                            rows['comment'],
                            rows['commentedUserName'],
                            rows['videoId'],
                            rows['channelId'],
                            rows['commentpublishedAt'])

                            cursor.execute("SELECT * FROM commentsDetail WHERE commentID=?", (rows['commentID'],))
                            existingrecord = cursor.fetchone()
                            if not existingrecord:
                                insertQuery = '''insert into commentsDetail(commentID, comment, commentedUserName, videoId, 
                                channelId, commentpublishedAt)
                                values(?, ?, ?, ?, ?, ?)'''
                                cursor.execute(insertQuery, values)
                            else:
                                pass



def callingMainFun(channelID):
    getChannelDetailsResponse = getChannelDetails(channelID)
    insertingData = insertToMongoDB(getChannelDetailsResponse['videodetailslist'])
    channelTableCreationObj = channelTableCreation()
    inserttochanneltableobj = inserttochanneltable(insertingData['channelList'])
    videosTableCreationObj = videosTableCreation()
    inserttovideostableobj = inserttovideostable(insertingData['dbCollectionName'], getChannelDetailsResponse['title'])
    commentsTableCreationObj = commentsTableCreation()
    inserttocommentstableobj = inserttocommentstable(insertingData['dbCollectionName'], getChannelDetailsResponse['title'])
    conn.commit()
    channeldetailsStShowobj = channeldetailsStShow(insertingData['dbCollectionName'], getChannelDetailsResponse['title'], insertingData['channelList'])
    videosDetailsStShowobj = videosDetailsStShow(insertingData['dbCollectionName'], getChannelDetailsResponse['title'], insertingData['channelList']) 
    commentsDetailsStShowobj = commentsDetailsStShow(insertingData['dbCollectionName'], getChannelDetailsResponse['title'], insertingData['channelList'])
    getExistingDetails()
    

def getExistingDetails():
    tablePresent = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' AND name='videosDetail';", conn)
    if tablePresent.empty:
        pass
    else:
        videosDetailsDF = pd.read_sql_query('''select vd.title as "Video Title", cd.title as "Channel Name" from videosDetail as vd
        left join channelDetail as cd on cd.channelID = vd.channelID''', conn)
        st.subheader('Videos Name And Channels Name', divider='rainbow')
        videosDetailsst = st.dataframe(data=videosDetailsDF, hide_index=True)

        maxVideosDF = pd.read_sql_query('select max(totalVideos), title from channelDetail', conn)
        maxVideos = maxVideosDF.iloc[0, 0]
        maxVideosChannelName = maxVideosDF.iloc[0, 1]
        st.subheader('Channels having the most number of Videos', divider='rainbow')
        st.metric(label=maxVideosChannelName, value=str(maxVideos) + ' ' + 'Vidoes')
    
        top10Videos = pd.read_sql_query('''select vd.thumbnails, vd.title as "Video Title", cd.title as "Channel Title", 
        vd.channelID, vd.videoID, vd.publishedAt, vd.totalLikes, vd.totalComments,
        vd.totalViews, vd.duration
        from videosDetail as vd join channelDetail as cd on  vd.channelID = cd.channelID 
        order by vd.totalViews desc limit 10''', conn)
        
        top10Videosdf = pd.DataFrame(data=top10Videos)
        st.subheader('Top 10 Videos Details', divider='rainbow')
        st.data_editor(
        top10Videosdf,
        column_config={
             "thumbnails": st.column_config.ImageColumn(
                "Thumbnails", help="Streamlit app preview screenshots"
            ),
            "title": "Title",
            "channelID": "Channel ID",
            "videoID": "Video ID",
            "publishedAt": "Published At",
            "totalLikes": "Total Likes",
            "totalComments": "Total Comments",
            "totalViews": "Total Views",
            "duration": "Duration",
            "favoriteCount": "Favourite Count",
            "caption": "Caption"
        },
        hide_index=True,
        )
        
        # How many comments were made on each video, and what are their corresponding video names?
        commentsCount = pd.read_sql_query('''select vd.title as 'Title', count(cd.videoId) as 'Comments Count' from commentsDetail as cd
        join videosDetail as vd on cd.videoId = vd.videoID group by cd.videoId''', conn)
        st.subheader('Videos Name And Comments Count', divider='rainbow')
        st.dataframe(data=commentsCount, hide_index=True)
        
        # Which videos have the highest number of likes, and what are their corresponding channel names?
        highestLikedVideos = pd.read_sql_query('''select max(vd.totalLikes) as "Highest Likes", vd.title as "Video Title", 
        cd.title as "Channel Title" from videosDetail as vd 
        join channelDetail as cd on vd.channelID = cd.channelID''', conn)
        vdName = highestLikedVideos.iloc[0, 1]
        chName = highestLikedVideos.iloc[0, 2]
        st.subheader('Video having the highest number of likes and Channel Name', divider='rainbow')
        st.metric(label=vdName + ' - Video Name', value=chName + ' - Channel Name')
        
        # What is the total number of likes and dislikes for each video, and what are their corresponding video names?
        numberOfLikesVdName = pd.read_sql_query('''select title as 'Videos Name', totalLikes as 'Number Of Likes' from videosDetail''', conn)
        st.subheader('Videos Name and Number of likes', divider='rainbow')
        st.dataframe(data=numberOfLikesVdName, hide_index=True)
        
        # What is the total number of views for each channel, and what are their corresponding channel names?
        numberOfViewsChName = pd.read_sql_query('''select title as 'Channel Name', totalViews as "Total Views" from channelDetail''', conn)
        st.subheader('Channels Name and Total Views', divider='rainbow')
        st.dataframe(data=numberOfViewsChName, hide_index=True)
        
        # What are the names of all the channels that have published videos in the year 2022?
        chName2022 = pd.read_sql_query('''SELECT DISTINCT ch.title AS 'Channel Name' 
        FROM channelDetail ch
        JOIN videosDetail vd ON ch.channelID = vd.channelID
        WHERE SUBSTR(vd.publishedAt, 1, 4) = "2022"''', conn)
        st.subheader('Channels Published in the year 2022', divider='rainbow')
        st.dataframe(data=chName2022, hide_index=True)
        
        # What is the average duration of the all vidoes in each channels and what are their corresponding channel names?
        sqlQuery = '''select ch.title as 'Channel Name', vd.duration as 'Duration' from videosDetail as vd
        join channelDetail as ch on vd.channelID = ch.channelID'''
        averageDuration = pd.read_sql_query(sqlQuery, conn)
        averageDurationdf = pd.DataFrame(averageDuration)
        averageDurationdf['Duration'] = pd.to_timedelta(averageDurationdf['Duration'])
        averageDurationdfAverage = averageDurationdf.groupby('Channel Name')['Duration'].mean()
        st.subheader('Average Duration of all the Channels', divider='rainbow')
        df = pd.DataFrame(averageDurationdfAverage)
        st.write(df.style.format({'duration': lambda x: str(x)}))

        
        # Which videos have the highest number of comments, and what are their corresponding channel names?
        numberOfComments = pd.read_sql_query('''select ch.title as "channel Name", vd.title as "Video Name", 
        count(cd.videoId) as "Highest number of comments" from 
        commentsDetail as cd join channelDetail as ch on cd.channelId = ch.channelId join videosDetail as vd
        on cd.videoID = vd.videoID group by cd.videoId order by "Highest number of comments"
        desc limit 1''', conn)
        videoChName = numberOfComments.iloc[0, 0]
        videoVdName = numberOfComments.iloc[0, 1]
        st.subheader('Video having the highest number of Commenst and Channel Name', divider='rainbow')
        st.metric(label=videoVdName + ' - Video Name', value=videoChName + ' - Channel Name')

    
def givechannelDetails():
    
    st.markdown("""<div style="text-align:center"><h2>Channel DashBoard</h2></div>""", unsafe_allow_html=True)
    with st.sidebar:
        st.title(":red[Youtube Data Harvesting and Warehousing]")
        st.caption("Give Channel ID To Get Channel Details")
        st.caption("All The Channel Details will be shown")
    channelID = st.text_input("Enter Channel ID")
    getDetailsObj = st.button('Submit')
    if getDetailsObj:
        callingMainFun(channelID)
        
def channeldetailsStShow(dbCollectionName, title, insertingData):
    
    #     creating streamlit application
    dbCollectionName = dbCollectionName
    title = title
    insertingData = insertingData
    

    val = list(insertingData[0].values())
    dfDcit = {}
    for key, value in enumerate(val[0]):
        dfDcit[value] = [val[0][value]]
    print(dfDcit)
    col1, col2, col3 = st.columns([4, 4, 4])
    with col2:
        st.image(dfDcit['logo'], caption='Channel Logo')
    channeldetailsdf = pd.DataFrame(data=dfDcit)
    st.subheader('Channel Details', divider='rainbow')
    st.data_editor(
    channeldetailsdf,
    column_config={
        "logo": st.column_config.ImageColumn(
            "Logo", help="Streamlit app preview screenshots"
        ),
        "title": "Tile",
        "channelID": "Channel ID",
        "description": "Description",
        "startDate": "startDate",
        "totalSubscribers": "Total Subscribers",
        "totalVideos": "Total Videos",
        "totalViews": "Total Views",
        "playlistID": "Playlist ID"
    },
    hide_index=True,
    )
    
def videosDetailsStShow(dbCollectionName, title, insertingData):
    
    dbCollectionName = dbCollectionName
    title = title
    insertingData = insertingData
    
    tempvideoliststdf = []
    for x in dbCollectionName.find({},{'_id':0, title:0}):
        tempvideolist = list(x.values())
        tempdict = {}
        if len(tempvideolist)!=0:
            for y in tempvideolist:
                for key, val in enumerate(y):
                    tempdict[val] = [tempvideolist[0][val]]
        if tempdict!={}:
            tempdict.pop('comments')
            tempdict['thumbnails'] = tempdict['thumbnails'][0]
            tempvideoliststdf.append(tempdict)
    videodetailsdf = pd.DataFrame(data=tempvideoliststdf)
    st.subheader('Videos Details', divider='rainbow')
    st.data_editor(
    videodetailsdf,
    column_config={
         "thumbnails": st.column_config.ImageColumn(
            "Thumbnails", help="Streamlit app preview screenshots"
        ),
        "title": "Title",
        "channelID": "Channel ID",
        "videoID": "Video ID",
        "publishedAt": "Published At",
        "totalLikes": "Total Likes",
        "totalComments": "Total Comments",
        "totalViews": "Total Views",
        "duration": "Duration",
        "tags": "Tags",
        "favoriteCount": "Favourite Count",
        "caption": "Caption"
    },
    hide_index=True,
    )
    
def commentsDetailsStShow(dbCollectionName, title, insertingData):
    
    dbCollectionName = dbCollectionName
    title = title
    insertingData = insertingData

    tempcommentliststdf = []
    for x in dbCollectionName.find({},{'_id':0, title:0}):
        tempvideolist = list(x.values())
        if len(tempvideolist)!=0:
            temparr = [tempvideolist[0]['comments']]
            for y in temparr:
                if y!={}:
                    for key, val in enumerate(y):
                        tempcommentliststdf.append(y[val])
    st.subheader('Comments Details', divider='rainbow')
    commentdetailsdf = st.dataframe(data=tempcommentliststdf)
    
givechannelDetails()



    
# conn.commit()
# UCXbz9MMVjZHx8dTXZ0Q-CYg - be positive wih jesus christ
# UCPO0ZAKhPg1_I9kYqTKcx3g - bruno the clever
# UC94VhYS0Mt8EfopFaV-vpaQ - Petslife 360
# UC9wn8eFRs4_bM7BN_9HICiA - Zid Collections