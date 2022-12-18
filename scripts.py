#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#Written by Timothy Fleck
#Started early 2022
""" These are various scripts used for communicating between csv files
and the database."""

#%% Set up the connection to the database
import psycopg2
import csv
import pandas
conn = psycopg2.connect(host="localhost",database="grocery1")
curs=conn.cursor() 
#%% Reset connection to the database
try:
    conn.close()
except:
    pass
conn = psycopg2.connect(host="localhost",database="grocery1")
curs=conn.cursor()
    
#%% Insert data from csv into database
""" This moves csv data into a database table.
The db table and the csv file must have the same name.
Each column in the csv files matches a column in the db table,
except there is no id column in the csv file. The id column is
filled with the default value.""" 

    
#collect the data from the csv files
target_table="recipes"
filename=target_table + ".csv"
readfile=open(filename,encoding="utf-8")
readobj=csv.reader(readfile,delimiter="\t")
next(readobj)#eliminates the heading from the csv file

#create a long string of psql insert statements with default id
count=0
commands=""
for row in readobj:
    count+=1
    commands = (commands + "insert into " + target_table +
                " values (default, '" + "','".join(row) + "');")

#%%check and execute the SQL commands
print(commands)
curs.execute(commands)
rows=str(count+2)
curs.execute("select * FROM " + target_table + " ORDER BY id DESC LIMIT " + rows + ";")
fetch=curs.fetchall()
print(fetch)
conn.commit()

#%%Find recipe ingredients in the database
missing=[]
rec=pandas.read_csv("recipes.csv",delimiter="\t")
for item in rec["ingredient_id"]:
    try:
        float(item) #check if it is a number matching an ingredient id
    except:
        stritem=str(item)
        #Look for an exact match
        command="select id,name,unit from ingredients where name = '" + stritem + "';"
        curs.execute(command)
        result=curs.fetchall()
        if result:
            print(result)
        else: #Look for a near match
            command= "select id,name,unit from ingredients where name like '%" + stritem + "%';"
            curs.execute(command)
            result=curs.fetchall()
            text = "No entry for " + stritem + "."
            if result:
                text+= " Try these suggestions:\n"
                print(text)
                print(result)
            else:
                print(text)
            missing.append(stritem)
print(missing)

#%% fill one column based on lookup of an id from another column
import pandas
pdf=pandas.read_csv("mb_match.csv",delimiter="\t")
for iid,mbid in zip(pdf["id"],pdf["mb_id"]):
    if int(iid)>0 and int(mbid)>0:
        command="update ingredients set marketbasket_id = " + str(mbid) + \
        " where id = " + str(iid) + ";"
        print(command)
        curs.execute(command)
command="select * from ingredients order by id desc limit 10"
curs.execute(command)
newlines=curs.fetchall()
print(newlines)
#conn.commit()

#%%Fill a new column from entries in a csv
import pandas
idf=pandas.read_csv("ingredients.csv",delimiter="\t")
z=zip(idf["id"],idf["pantry_location"])
for e in z:
    command= "update ingredients set pantry_location = '" \
    + str(e[1]) + "' where id = " + str(e[0]) + ";"
    curs.execute(command)
#conn.commit()

#%% Get all ingredients needed for a single dish
dish_id=7
command= "select \
ingredients.name, recipes.quantity, ingredients.unit from \
recipes join ingredients on recipes.ingredient_id = ingredients.id \
where recipes.dish_id=" + str(dish_id)+";"
curs.execute(command)
stuff=curs.fetchall()
for thing in stuff:
    print((thing[0] + ":  " + str(float(thing[1])) + " " + thing[2]))

#%% Get all places where an ingredient is used
ingredient_name = "olive oil"
command = "select \
recipes.id, recipes.ingredient_id,ingredients.name,recipes.quantity, \
ingredients.unit, dishes.name as dish from \
recipes join dishes on recipes.dish_id = dishes.id \
join ingredients on recipes.ingredient_id = ingredients.id \
where recipes.ingredient_id in \
(select id from ingredients where name like '%" + ingredient_name + "%');"
curs.execute(command)
stuff=curs.fetchall()
print(stuff)

#%% backup each table
table_names = ["dishes","ingredients","recipes","pantry","shaws","wegmans",
               "traderjoes","marketbasket"]
for table_name in table_names:
    file_name = table_name + "_backup.csv"
    command = "select * from " + table_name + " order by id;"
    curs.execute(command)
    headings=[des[0] for des in curs.description]
    backup_table=curs.fetchall()
    backup_df=pandas.DataFrame(backup_table)
    backup_df.columns=headings
    if "id" in headings:
        backup_df=backup_df.set_index("id")
    backup_df.to_csv(file_name,sep="\t")

#%% Old query without temporary table
command = \
"SELECT SUM(dishes.quantity*recipes.quantity) amount, \
ingredients.unit, ingredients.name, ingredients.id \
FROM dishes \
JOIN recipes ON dishes.id = recipes.dish_id \
JOIN ingredients ON recipes.ingredient_id = ingredients.id \
JOIN pantry ON ingredients.pantry_id = pantry.id \
WHERE dishes.quantity>0 \
GROUP BY ingredients.id,sequence \
ORDER BY sequence;"
curs.execute(command)
stuff=curs.fetchall()