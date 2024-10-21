# import mysql.connector
# import json

# db = mysql.connector.connect(
#     host="localhost",
#     user="root",  
#     password="1234",  
#     database="json_assignment"
# )
# with open('sample_data_for_assignment.json', 'r', encoding='utf-8', errors='replace') as f:
#     json_data = json.load(f)
    

# print(json_data)

# # Extract columns and data from JSON
# cols = json_data['cols']
# data = json_data['data']

# cursor = db.cursor()

# query = f"INSERT INTO json_to_sql_table ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"

# for row in data:
#     cursor.execute(query, row)
    
# db.commit()

# print(f"{cursor.rowcount} rows inserted successfully.")

# 
# cursor.close()
# db.close()

# --------------------------------------------------------------------------------------------------
import mysql.connector
import json

try:
    
    db = mysql.connector.connect(
        host="localhost",
        user="root",  
        password="1234",  
        database="json_assignment"
    )
    print("Connected to the database successfully.")

    try:
        with open('sample_data_for_assignment.json', 'r', encoding='utf-8', errors='replace') as f:
            json_data = json.load(f)
        print("JSON file loaded successfully.")
        
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading JSON file: {e}")
        raise  
    
    print(json_data['cols'])
    cols = json_data['cols']
    data = json_data['data']

    cursor = db.cursor()
    query = f"INSERT INTO json_to_sql_table ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"

    try:
        for row in data:
            cursor.execute(query, row)
        db.commit()  
        print(f"{cursor.rowcount} rows inserted successfully.")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
        db.rollback()  

except mysql.connector.Error as e:
    print(f"Error connecting to the database: {e}")

finally:
    
    if 'cursor' in locals():
        cursor.close()
        print("Cursor closed.")
    if 'db' in locals() and db.is_connected():
        db.close()
        print("Database connection closed.")