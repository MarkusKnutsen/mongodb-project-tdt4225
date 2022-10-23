from DbConnector import DbConnector
from haversine import haversine
from datetime import datetime, timedelta


class query:

  def __init__(self):
      self.connection = DbConnector()
      self.client = self.connection.client
      self.db = self.connection.db
  
  def number_of_entries(self):
    collection = self.db["User"]
    users = collection.count_documents({})
    collection = self.db["Activity"]
    activities = collection.count_documents({})
    collection = self.db["TrackPoint"]
    trackpoints = collection.count_documents({})
    print("| Users: ", users, "| Activities: ", activities, "| TrackPoints: ", trackpoints, "|")

  def average_number_of_activities(self):
    collection = self.db["User"]
    users = collection.count_documents({})
    collection = self.db["Activity"]
    activities = collection.count_documents({})
    print("| Average number of Activities per user: ", "{:.4f}".format(activities/users), "|")

  def most_activities(self):
    collection = self.db["User"]
    query = [{"$unwind": "$activities"}, { "$group" : {"_id":'$_id', "result":{"$sum":1}}}, { "$sort" :{ "result": -1}}, {"$limit": 20}]
    documents = collection.aggregate(query)
    print("The 20 Users with the most Activities\n\
-------------------------------------\
\n\nRank | User | Activities\n")
    count = 0
    for doc in documents:
      count += 1
      print(f"{count:4} | {doc['_id']:4}   {doc['result']}")

  def has_taken_taxi(self):
    collection = self.db["Activity"]
    users = collection.distinct("user_id", {"transportation_mode": "taxi"})
    print("Users who has taken a taxi\n\
--------------------------\
\n\nUser\n")
    for user in users:
      print(user)

  def number_of_transportation_modes(self):
    collection = self.db["Activity"]
    query = [{"$group" : { "_id" : '$transportation_mode', "count" : {"$sum" : 1}}}, { "$sort" :{ "count": -1}}]
    documents = collection.aggregate(query)
    print("Amount of times the transportation modes were tagged in an activity \n\
-------------------------------------------------------------------\
\n\nTransport | Activities\n")
    for doc in documents:
      print(f"{doc['_id']:9} | {doc['count']:6d}")

  def year_with_activities(self):
    collection = self.db["Activity"]
    query = [{"$group" : { "_id" : {"$year": {"$toDate": '$start_date_time'}}, "count" : {"$sum" : 1}}}, { "$sort" :{ "count": -1}}]
    documents = collection.aggregate(query)
    print("\nHow many Activities that were registered in the different years\n\
---------------------------------------------------------------\
\n\nYear | Activities\n")
    for doc in documents:
      print(f"{doc['_id']:4} | {doc['count']}")

  def hours_of_activities_per_year(self):
    collection = self.db["Activity"]
    query = [{"$group" : { "_id" : {"$year": {"$toDate": '$start_date_time'}}, "count" : {"$sum" : {"$divide": [{"$dateDiff": {"startDate": {"$toDate": '$start_date_time'}, "endDate": {"$toDate": '$end_date_time'}, "unit": "second"}}, 3600]}}}}, { "$sort" :{ "count": -1}}]
    documents = collection.aggregate(query)
    print("\nHow many hours of Activities that were registered in the different years\n\
------------------------------------------------------------------------\
\n\nYear | Hours\n")
    for doc in documents:
      print(f"{doc['_id']:4} | {doc['count']:.1f}")
  
  def distance_user_112(self):
    distance = 0
    collection = self.db["Activity"]
    query = [{ "$match" : { "user_id" : 112, "start_date_time": {"$regex": "2008"}, "transportation_mode": "walk"} }, {"$lookup": {"from": "TrackPoint", "localField": "trackpoints", "foreignField": "_id", "as": "trackpoints"}}, {"$project": {"_id": 0, "trackpoints": {"$sortArray": {"input": "$trackpoints", "sortBy": {"_id": 1}}}}}]
    documents = collection.aggregate(query)
    for doc in documents:
      for i in range(len(doc["trackpoints"])-1):
        pos1 = (doc["trackpoints"][i]["lat"], doc["trackpoints"][i]["lon"])
        pos2 = (doc["trackpoints"][i+1]["lat"], doc["trackpoints"][i+1]["lon"])
        distance += haversine(pos1, pos2)

    print("The distance walked by User 112 in 2008 was", round(distance, 2), "km.")

  def most_altitude(self):
    altitude = {}
    collection = self.db["Activity"]
    query = [{"$lookup": {"from": "TrackPoint", "localField": "trackpoints", "foreignField": "_id", "as": "trackpoints"}}, {"$project": {"user_id": 1, "_id": 0, "trackpoints": {"$sortArray": {"input": "$trackpoints", "sortBy": {"_id": 1}}}}}]
    documents = collection.aggregate(query)
    for doc in documents:
      for i in range(len(doc["trackpoints"])-1):
        user = doc[("user_id")]
        if not (doc["trackpoints"][i]["altitude"] == -777 or doc["trackpoints"][i+1]["altitude"] == -777):
          altitude_part = max( ( doc["trackpoints"][i+1]["altitude"] - doc["trackpoints"][i]["altitude"] )*0.3048 , 0 )
          if doc["user_id"] in altitude:
            altitude[user] += altitude_part
          else:
            altitude[user] = altitude_part 
    print("Top 20 Users that gained the most altitude\n\
----------------------------------\
\n\nRank | User | Altitude gain\n")
    count=1
    for item in sorted(altitude, key=altitude.get, reverse=True):
      if count == 21: break
      print(f"{count:4} | {item:5} {round(altitude[item]):8}")
      count += 1
  
  def invalid_activities(self):
    errors = {}
    collection = self.db["Activity"]
    query = [{"$lookup": {"from": "TrackPoint", "localField": "trackpoints", "foreignField": "_id", "as": "trackpoints"}}, {"$project": {"user_id": 1, "activity_id": 1, "_id": 0, "trackpoints": {"$sortArray": {"input": "$trackpoints", "sortBy": {"_id": 1}}}}}]
    documents = collection.aggregate(query)
    for doc in documents:
      user = doc["user_id"]
      for i in range(len(doc["trackpoints"])-1):
        if (((datetime.fromisoformat(doc["trackpoints"][i+1]["date_time"])) - (datetime.fromisoformat(doc["trackpoints"][i]["date_time"]))).total_seconds()/60) >= 5:
          if doc["user_id"] in errors:
            errors[user] += 1
          else:
            errors[user] = 1
          break
    print("The Users that have invalid Activities\n\
----------------------------------\
\n\nUser | Invalid Activities\n")
    for item in sorted(errors, key=errors.get, reverse=True):
      print(f"{item:5} {round(errors[item]):8}")

  def forbidden_city(self):
    collection = self.db["TrackPoint"]
    query = {"$and": [{"lat": 39.916}, {"lon": 116.397}]}
    documents = collection.find(query)
    print("The Users who have registered an activity in the Forbidden City of Beijing: ")
    for doc in documents:
      print(doc)
  
  def most_used_transportation(self):
    collection = self.db["Activity"]
    query = [{ "$group": { "_id": { "transportation_mode": "$transportation_mode","user_id": "$user_id" }, "count": { "$sum":1 } }}, { "$sort": { "_id.user_id": 1, "count":-1 }}, { "$group": { "_id": {"user_id": "$_id.user_id"}, "transportation_mode": {"$first": "$_id.transportation_mode"} ,"count": { "$first": "$count" } }}, { "$sort": { "_id.user_id": 1, "count":-1 }}]
    documents = collection.aggregate(query)
    print("The most frequently used transportation mode in the Activities of the Users\n\
---------------------------------------------------------------------------\
\n\nUser | Most used transportation mode\n")
    for doc in documents:
      print(f"{doc['_id']['user_id']:4} | {doc['transportation_mode']:4}")


db = query()
db.number_of_entries()
print("\n\n\n")
db.average_number_of_activities()
print("\n\n\n")
db.most_activities()
print("\n\n\n")
db.has_taken_taxi()
print("\n\n\n")
db.number_of_transportation_modes()
print("\n\n\n")
db.year_with_activities()
print("\n\n\n")
db.hours_of_activities_per_year()
print("\n\n\n")
db.distance_user_112()
print("\n\n\n")
db.most_altitude()
print("\n\n\n")
db.invalid_activities()
print("\n\n\n")
db.forbidden_city()
print("\n\n\n")
db.most_used_transportation()