import datetime
import pymongo
from flask import Flask,redirect
import numpy as np
import pandas as pd
import gspread
import json
import requests
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
from bson import ObjectId
from gspread_dataframe import get_as_dataframe, set_with_dataframe
#Converting in Year Month Day Hour Minute Second format

app = Flask(__name__)
@app.route('/')
def home():
    x = datetime.datetime(2021, 12, 1, 1, 0, 0)

    #Connecting to Mongodb
    dbConn = pymongo.MongoClient("mongodb://readMarketingUserProd:exp233sjbsjse@15.207.12.59:27017/")
    #Extracting from expertron on mongodb
    mydb = dbConn['expertrons']

    #giving the  name that need to accessed, 'product_orders' collection contains all the sales punch order
    #Candidate details not present in product orders
    collection_name1 = 'product_orders'
    #Accessing the collection(Tables)
    collection1 = mydb[collection_name1]
    #Extracting all entries of columns stated below
    result = collection1.find({'academyCourseId':ObjectId('62b76350594cd1d7f55797f1')}, {'userId','orderId','cpType','paidAmount','academyCourseId','createdAt'})
    result1 = collection1.find({'academyCourseId':ObjectId('62b75cde96b04ed7ca2649a3')}, {'userId','orderId','cpType','paidAmount','academyCourseId','createdAt'})
    #normalising json data into flat table
    users = pd.concat([pd.json_normalize(result),pd.json_normalize(result1)])
    #when we access data from mongodb by default _id is created auto generated,
    #and we are removing that column
    users.rename(columns={'_id': 'productOrderId'}, inplace=True)

    #for getting aspirant details
    collection_name1 = 'users'
    collection1 = mydb[collection_name1]
    result = collection1.find({}, {'_id','fullname','email', 'mobile', 'cityid'})
    user = pd.json_normalize(result)
    #Renaming the "_id" column as "userID"
    user.rename(columns={'_id': 'userId'}, inplace=True)
    #Merging users ans user table with common column as userID
    user = pd.merge(users,user,on='userId')

    collection_name1 = 'academy_courses'
    collection1 = mydb[collection_name1]
    #Extracting column 'courseName' and storing it in result
    result = collection1.find({}, {'courseName'})
    #Normalizing the data into table format
    courses = pd.json_normalize(result)
    courses.rename(columns={'_id': 'academyCourseId'}, inplace=True)
    user = pd.merge(user,courses,on='academyCourseId')

    collection_name1 = 'cities'
    collection1 = mydb[collection_name1]
    #extracting "name" column and its entries then storing it in a variable result
    result = collection1.find({}, {"name"})
    city = pd.json_normalize(result)
    city.rename(columns={'_id': 'cityid'}, inplace=True)
    user = pd.merge(user,city,on='cityid',how='left')

    User = user[['createdAt','userId','orderId','fullname','mobile', 'email',"name",'courseName','paidAmount','cpType']]

    x = datetime.datetime(2022, 6, 18, 1, 0, 0)
    y = datetime.datetime(2022, 6, 30, 1, 0, 0)
    #Connecting to Mongodb
    dbConn = pymongo.MongoClient("mongodb://readMarketingUserProd:exp233sjbsjse@15.207.12.59:27017/")
    #Extracting from expertron on mongodb
    mydb = dbConn['expertrons']
    collection_name1 = 'oms_bubble'
    collection1 = mydb[collection_name1]
    result = collection1.find({"salesPunchDate":{"$gte":x}}, {'orderId','qualityStatus'})
    user = pd.json_normalize(result)
    user.drop(['_id'], inplace=True, axis=1)

    User = pd.merge(User,user,on='orderId')

    collection_name1='academy_candidates'
    collection1 = mydb[collection_name1]
    result = collection1.find({},{'candidateCode','userId'})
    academy_candidates = pd.json_normalize(result)
    academy_candidates.drop(['_id'], inplace=True, axis=1)
    User = pd.merge(User,academy_candidates,on='userId')

    User.rename(columns={'createdAt':'Timestamp','orderId':'Order ID','fullname':'Aspirant  Name','mobile':'Aspirant Contact','email':'Aspirant Email','name':'Aspirant Current City',
                         'courseName':'Program Type','paidAmount':'Paid Amount','cpType':'Type of Sale','qualityStatus':'Quality Status'},inplace=True)

    User = User[['Order ID','Timestamp','Aspirant  Name', 'Aspirant Contact',
           'Aspirant Email', 'Aspirant Current City', 'Program Type',
           'Paid Amount', 'Type of Sale', 'Quality Status', 'candidateCode']]

    User.fillna('',inplace=True)
    for _ in range(200):
      User = User.append(pd.Series(), ignore_index=True)

    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_file('sample.json', scopes=scopes)
    gc = gspread.authorize(credentials)
    
    #Importing all the data from google sheets
    spreadsheet = gc.open_by_key('1SIQtKcit4DgqNiS5QIWMIMAZjq5IePNQlJXmfgHry6k')
    worksheet = spreadsheet.worksheet("Sheet1")
    set_with_dataframe(worksheet,User)
    return redirect("https://docs.google.com/spreadsheets/d/1SIQtKcit4DgqNiS5QIWMIMAZjq5IePNQlJXmfgHry6k/edit#gid=0")
if __name__ == "__main__":
    app.run(debug=True)