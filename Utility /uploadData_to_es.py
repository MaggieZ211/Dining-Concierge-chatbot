
import os
import pandas

#connect to open search and insert data using curl POST
def uploadData_to_es():
    CSVtable = pandas.read_csv('results.csv')
    for i in range(len(CSVtable)):
        #include Master and Password
        s1 = 'curl -X POST -u' + " '" + "MaggieZ98:Zll1998!" + "' "  
        #endpoint copy from aws open search console, add index('restaurants') and type('Restaurant')
        endpoint = "'" + "https://search-my-es-lqxrxfudgfafrhsyer22iqi3u4.us-east-1.es.amazonaws.com/restaurants/Restaurant" + "' " 
        #Upload only {Business_ID, Cuisine} in es for further searching and recommendation
        body = '-d' + " '" + "{" + '"Business_ID": "{}", "Cuisine": "{}"'.format(CSVtable['Business_ID'][i], CSVtable['Cuisine'][i]) + "}" + "'" + " "
        header = '--header' + " '" + "Content-Type: application/json" + "'"

        os.system(s1 + endpoint + body + header)

uploadData_to_es()
