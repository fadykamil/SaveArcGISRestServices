import json
import os
import os.path
from datetime import datetime, timedelta
import psycopg2
import requests


#################################################
# Variables:
############
objectIdsJson='arabicData/terrestrial_Protected_Areas/objectIds.json'  # path to the dir of json file of object ids
# ArcGIS server Rest URL
url = "http://emisk.org/ArcGIS/rest/services/Public_Domain_Data_Arabic/MapServer/19"

DIR = 'arabicData/terrestrial_Protected_Areas/jsonDir'  # path to directory where th json files are generated (it must be empty)

#Connection string variables
dbname = 'kuwait_db_arabic'
user='postgres'
host='localhost'
password='clogic'

table_name = 'terrestrial_Protected_Areas '  # DB table which must have columns with the same name and data types as in the ArcGIS Layer

maxFeatures = 9
##################################################
# Functions
def splice(lst, num):
    if num > 0:
        x = lst[0:num]
        lst[0:num] = []
        return x
    else:
        print("Second argument must be more than 0")


def convArrToStrCommaDel(arr):
    x = ""
    for index in range(len(arr)):
        x += str(arr[index]) + ","

    return x[:-1]

url += "/query?"
# load json file in data obj
with open(objectIdsJson) as data_file:
    dataIds = json.load(data_file)
i = 1
objectIds = dataIds["objectIds"]
print len(objectIds)
while len(objectIds) > maxFeatures:
    print i
    idsStr = convArrToStrCommaDel(splice(objectIds, maxFeatures))

    params = {'where': '1=1', 'objectIds': idsStr, 'f': 'json', 'outFields': '*'}

    req = requests.request("POST", url, data = params)
    #print req.url
    featuresJson = req.json()

    with open(DIR+'/features' + str(i) + '.json', 'w') as outfile:
        json.dump(featuresJson, outfile)
    i += 1

idsStr = convArrToStrCommaDel(splice(objectIds, len(objectIds)))
params = {'where': '1=1', 'objectIds': idsStr, 'f': 'json', 'outFields': '*'}

req = requests.request("POST", url, data = params)
featuresJson = req.json()
with open(DIR+'/features' + str(i) + '.json', 'w') as outfile:
    json.dump(featuresJson, outfile)

##################################################
try:
    conn = psycopg2.connect("dbname='"+dbname+"' user='"+user+"' host='"+host+"' password='"+password+"'")
except:
    print "I am unable to connect to the database"

cur = conn.cursor()
filesNumber= len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])
for j in range(filesNumber):
    print j
    with open(DIR+'/features' + str(j+1) + '.json') as data_file:
        data = json.load(data_file)
    features = data["features"]
    fields = data["fields"]

    for feature in features:
        cols = []
        vals = []
        for field in fields:
            if field["name"] in ["SHAPE_Length"]:
                continue
            if field["type"] in ['esriFieldTypeOID', 'esriFieldTypeGlobalID']:
                continue
            key = field["name"]
            attributes = feature["attributes"]
            val = attributes[key]
            if val is None:
                val = 'null'
            elif field["type"] in ["esriFieldTypeString"]:
                val = "'%s'" % val
                val = "$$" + val + "$$"
            elif field["type"] in ["esriFieldTypeDate"]:
                val = "'%s'" % (datetime(1970, 1, 1) + timedelta(milliseconds = val))
            else:
                val = str(val)
            cols.append(key)
            vals.append(val)  # take care of type
        cols.append('geometry')
        geomType = data["geometryType"]

        if geomType in ["esriGeometryPolygon"]:
            if 'geometry' in feature:
                if feature["geometry"] is not None and feature["geometry"]["rings"] is not None:
                    geometry = feature["geometry"]["rings"]
                    # POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))
	                # MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))
                    if len(geometry) > 1:
                        geometryWKT = "MULTIPOLYGON ((("
                    else:
                        geometryWKT = "POLYGON (("

                    for index1 in range(len(geometry)):
                        if index1 != 0 :
                            geometryWKT += ")),(("
                        for index2 in range(len(geometry[index1])):
                            if index2 != 0:
                                geometryWKT += ","
                            for index3 in range(len(geometry[index1][index2])):
                                if index3 == 1:
                                    geometryWKT += str(geometry[index1][index2][index3])
                                else:
                                    geometryWKT += str(geometry[index1][index2][index3]) + " "
                    if len(geometry) > 1:
                        geometryWKT += ")))"
                    else:
                        geometryWKT += "))"
        ##################################################
        elif geomType in ["esriGeometryPolyline"]:
            if 'geometry' in feature:
                if feature["geometry"] is not None and feature["geometry"]["paths"] is not None:
                    geometry = feature["geometry"]["paths"]
                    # LINESTRING (30 10, 10 30, 40 40)
	                # MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))
                    if len(geometry) > 1:
                        geometryWKT = "MULTILINESTRING (("
                    else:
                        geometryWKT = "LINESTRING ("

                    for index1 in range(len(geometry)):
                        if index1 != 0 :
                            geometryWKT += ")),(("
                        for index2 in range(len(geometry[index1])):
                            if index2 != 0:
                                geometryWKT += ","
                            for index3 in range(len(geometry[index1][index2])):
                                if index3 == 1:
                                    geometryWKT += str(geometry[index1][index2][index3])
                                else:
                                    geometryWKT += str(geometry[index1][index2][index3]) + " "
                    if len(geometry) > 1:
                        geometryWKT += "))"
                    else:
                        geometryWKT += ")"
        ############################################
        elif geomType in ["esriGeometryPoint"]:
            if 'geometry' in feature:
                if feature["geometry"] is not None:
                    geometry = feature["geometry"]
                    #POINT (30 10)
                    geometryWKT = "POINT ("
                    geometryWKT += str(geometry["x"]) + " "
                    geometryWKT += str(geometry["y"]) + ")"
            else:
                continue

        vals.append("ST_GeomFromText('" + geometryWKT + "')")
        insert_sql = \
            "insert into %s (%s) values(%s)" % (table_name, ",".join(cols), ",".join(vals))
        cur.execute(insert_sql)
    conn.commit()
    print "File " +str(j) + " is done"

conn.commit()
print "Records created successfully"
conn.close()
