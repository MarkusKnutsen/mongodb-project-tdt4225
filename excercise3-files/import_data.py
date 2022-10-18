from process_data import *
from datetime import datetime
from ActivityDB import ActivityDB
from timeit import default_timer as timer
import warnings

#Used so to not have a warning that pollutes the output every time.
warnings.simplefilter(action='ignore', category=FutureWarning)

data_, transport_ = process_data('dataset\dataset\Data')

#The data list will only contain the datetime objects of all the tracking points.
data = []

#The transport list will contain all the activities that are valid, i.e all the activities that have
#registered an exact match in start- and end date.
transport = []

#Making the lists have a spot for all the 181 users in this dataset.
for i in range(182):
    data.append([])
    transport.append([])

print('Creating the data array of dates.')
for i in range(len(data_)):

#Removing the data from people who have not recoded activities.
  if transport_[i] == []:
    data_[i] = []
  else:
    for item in data_[i]:
      for time in item:

#Making a new datalist where we have a datetime obect, and not two separated date and time objects.
        data[i].append(datetime.combine(time["Date"], time["Time"]))

print("Filtering for only activities with proper dates.")
for i in range(len(transport_)):

#Problem with iteration if it was in size 1.
  if transport_[i] != []:

#Checking if the datetime of the start and stop of the activity is recorded in the trajectories
#and if they are then they are stored in the new transportation list, where only the trasportations
#with corresponding trajectory points are.
    if (transport_[i]).size == 1:
      if (transport_[i]["start_date"] in data[i]) and (transport_[i]["end_date"] in data[i]):
        transport[i].append((transport_[i]["start_date"], transport_[i]["end_date"], transport_[i]["Transportation"]))
    else:
      for item in ((transport_[i])):
        if (item[0] in data[i]) and (item[1] in data[i]):
          transport[i].append(item)
#We now have all the recorded activities.

#Function for inserting the data into the database created in ActivityDB.py.
def main():
    db = None
    try:
        db = ActivityDB()

#Creating the collections 
        db.create_coll(collection_name="User")
        db.create_coll(collection_name="Activity")
        db.create_coll(collection_name="TrackPoint")
        
#Creating lists for the User, Activity and TrackPoint documents
        user_docs = []
        activity_docs = []
        trackpoint_docs = []

#We need to set an id for the Activities and the TrackPoints. I will manually set them, so that they are most likely not the same in any documents.
#The ID's for the Users are from 0-181, and therefore the Activity id's will start at a value above 181. The TrackPoint ID's will start at 
#500000, as that is a well enough margin. 
        activity_id = 200
        trackpoint_id = 500000

#Iterating through the Users
        for user in range(len(data)):
       
#Creating the array for the reference of the Activity IDs connected to the current User.
          activities = []

#Creating a flat list of the valid TrackPoints, instead of the TrackPoints being in lists containing to the files they were read from.
          f_data = [item for sublist in data_[user] for item in sublist]          

#Iteration variable. Used for tracking the index in the trajectory list. By doing this we do not need to read the already read entries
#and save computational time by doing so.
          iter = 0

#Iterating through the Activities of the user.
          if transport[user] != []:
            for activity in transport[user]:

#Creating the array for the reference of the TrackPoint IDs connected to the current Activity.
              trackpoints = []

#Adding the Activity ID to the reference list
              activities.append(trackpoint_id)

#A truth variable for verifying if the TrackPoint is within the activity interval.
              check = False

#Iterating the TrackPoints of the trajectory of the user.
              for trackpoint in data[user][iter:]:

#If the TrackPoint we are looking at is equal to the start time of the activity, we know we are inside the Activity interval, and
#we set the truth variable to True.
                if trackpoint == activity[0]:
                  check = True
              
                if check:
                  
#Adding the current Activity document to their list.
                  trackpoint_docs.append(
                    {
                      '_id': trackpoint_id,
                      'activity_id': activity_id,
                      'lat': f_data[iter]["Latitude"],
                      'lon': f_data[iter]["Longitude"],
                      'altitude': f_data[iter]["Altitude"],
                      'date_days': f_data[iter]["Days"],
                      'date_time': trackpoint.isoformat()
                    }
                  )

#Adding the TrackPoint ID to the reference list
                  trackpoints.append(trackpoint_id)

#Generating a new unique TrackPoint ID.
                  trackpoint_id += 1
              
#Updating the index.
                iter += 1

#If we have reached the end of the activity, i.a. the trackpoint is equal to the end time of the activity,
#we set the truth variable to False. We also break out of the loop, as there is nothing more to be found in this activity. 
                if trackpoint == activity[1]:
                  check = False
                  break

#Adding the current Activity document to their list, with all their TrackPoints.
              activity_docs.append(
                {
                  '_id': activity_id,
                  'user_id': user,
                  'transportation_mode': activity[2],
                  'start_date_time': activity[0].isoformat(),
                  'end_date_time': activity[1].isoformat(),
                  'trackpoints': trackpoints
                }
              )

#Generating a new unique Activity ID.
              activity_id +=1

#Adding the current user document to their list, with all their activities
          user_docs.append(
            {
              '_id': user,
              'activities': activities  
            }
          )
          print('User ', user, ' completed.')

#Inserting the data into their collections.        
        db.insert_documents(collection_name="User", docs=user_docs)
        db.insert_documents(collection_name="Activity", docs=activity_docs)
        db.insert_documents(collection_name="TrackPoint", docs=trackpoint_docs)
        db.show_coll()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
        db.drop_coll(collection_name="User")
        db.drop_coll(collection_name='Activity')
        db.drop_coll(collection_name='TrackPoint')
    finally:
        if db:
            db.connection.close_connection()


if __name__ == '__main__':
#Timing the function main()
  start = timer()
  main()
  end = timer()
  print("Elapsed time: ", end-start)

