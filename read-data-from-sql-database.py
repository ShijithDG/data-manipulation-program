import mysql.connector as connection
import pandas as pd

try :
    
    mydb = connection.connect(
        host = 'localhost',
        database = 'json_assignment',
        user = 'root',
        password = '1234'
    )

   
    query = 'SELECT * FROM json_to_sql_table'
    cursor = mydb.cursor(dictionary=True)
    query = 'SELECT * FROM json_to_sql_table LIMIT 2'
    cursor.execute(query)

    # Fetch the results
    print(cursor,'shijith')
    results = cursor.fetchall()

    # Print the results
    print(results)
    
    
    
except Exception as e:
    print(f'error is : {e}')
    
finally:
    mydb.close()
    print('everythis is closed')