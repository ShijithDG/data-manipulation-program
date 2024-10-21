import re
import pandas as pd
import mysql.connector as connector

try:
    mydb = connector.connect(
        host = 'localhost',
        user = 'root',
        password = '1234',
        database = 'json_assignment '
    )
    
    query = 'SELECT * FROM json_to_sql_table'

    result_dataframe = pd.read_sql(query, mydb)
    
    # print(result_dataframe)
    
    def manipulate_email(string):
    
        new_domain = '@gamil.com'
        new_email =''
        for i in range (len(string)):
            if string[i] == '@':
                break;
            new_email += string[i]
            
        return new_email+ new_domain
    
    def clean_postal_code(value):
        
        numeric_part = re.sub(r'\D', '', str(value))  # \D matches non-digit characters
        
        return int(numeric_part) if numeric_part else None
    
    def clean_phone_number(value):
        
        number_without_character = ''.join(re.findall(r'\d+', value))
        cleaned_number =  int(number_without_character) if number_without_character else None
        cleaned_number_string = str(cleaned_number)
        length = str(cleaned_number)
        result = ''

        for i in range(0, len(length)-1,2):
            
            two_digit = int(cleaned_number_string[i : i + 2])
            
            if two_digit < 65 :
                result += 'O'
            else:
                result += (chr(two_digit))
    
        return result        
            
    
    result_dataframe['email'] = result_dataframe['email'].apply(manipulate_email)
    result_dataframe['postalZip'] = result_dataframe['postalZip'].apply(clean_postal_code)
    result_dataframe['phone'] = result_dataframe['phone'].apply(clean_phone_number)

    print(result_dataframe['phone'])
    
except Exception as e:
    print(f"error is {e}")

finally:
    mydb.close()
    print('mysql conntection is closed')