#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#Written by Timothy Fleck
#started in February 2022
"""
This is the weekly script to create a grocery list. It should be run one cell
or one line at a time, depending on how much caution you want to take with
conn.commit() statements.


Steps:
    1. (offline) Start the database
    2. (offline) Modify a weekX.csv file with all the dishes needed this week.
        Make sure it's in the current working directory.
    3. Connect to the database.
    3. Update the dishes and ingredients tables with all the items needed.
    4. Write the ingredients needed to a .csv file.
    5. (offline) Modify the .csv file to remove items already on hand.
    6. Update the database based on the edited .csv file.
    7. Repeate steps 4-6 as needed.
"""

#%% initial setup
import psycopg2
import csv
import pandas
conn = psycopg2.connect(host="localhost",database="grocery1")
curs=conn.cursor()
#%% Set up weekly menu

#Set dish and ingredient quantities to 0
command = "update dishes set quantity = 0;update ingredients set amount = 0"
curs.execute(command)
conn.commit()

#Update dish quantities from csv file
menu_file="weekB.csv"
menu=pandas.read_csv(menu_file,delimiter="\t")
for q,i in zip(menu["quantity"],menu["dish_id"]):
    command="update dishes set quantity = " + str(q) + " where id=" + str(i) + ";"
    curs.execute(command)
conn.commit()

#Update ingredient quantities
command = \
"WITH ta AS ( \
SELECT ingredients.id iid,SUM(dishes.quantity * recipes.quantity) amount \
FROM dishes \
JOIN recipes ON dishes.id = recipes.dish_id \
JOIN ingredients ON recipes.ingredient_id = ingredients.id \
GROUP BY ingredients.id) \
UPDATE ingredients SET amount = ta.amount FROM ta WHERE id = ta.iid;"
curs.execute(command)
conn.commit()

#%% Write results to csv
store = "marketbasket"
command="select amount,unit,name,ingredients.id, " + store + ".description from ingredients \
JOIN " + store + " ON ingredients." + store + "_id = " + store + ".id \
WHERE amount>0 ORDER BY " + store + ".sequence;"
curs.execute(command)
stuff=curs.fetchall()
storef=store + "list.csv"
with open(storef,'w') as f:
    wf=csv.writer(f,delimiter="\t")
    wf.writerow(['amount','unit','description','id','location'])
    wf.writerows(stuff)
#%% Update ingredients with new amounts from csv
storef=store + "list.csv"
newq=pandas.read_csv(storef,delimiter="\t")
for q,i in zip(newq["amount"],newq["id"]):
    command="update ingredients set amount = " + str(q) + " where id=" + str(i) + ";"
    curs.execute(command)
conn.commit()