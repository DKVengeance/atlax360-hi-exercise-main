import json
import pyodbc

class DBExtractor():
    def __init__(self, configFile: str):
        f = open(configFile)
        data = json.load(f)
        
        self._HOST = data["HOST"]
        self._PORT = data["PORT"]
        self._DATABASE = data["DATABASE"]
        self._USER = data["USER"]
        self._PASSWORD = data["PASSWORD"]
        
        f.close()
 

    def extract(self, targetFile: str):
        conn = None
        
        try:
            # conn = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server}" +
            conn = pyodbc.connect("DRIVER={SQL Server}" +
                                ";SERVER=" + self._HOST + 
                                ";DATABASE=" + self._DATABASE + 
                                ";UID=" + self._USER + 
                                ";PWD=" + self._PASSWORD + 
                                ";TrustServerCertificate=Yes")
            
            # Insert your exercise code here
            import pandas.io.sql as pd
            from datetime import datetime
            cursor = conn.cursor()
            # cursor.execute("CREATE INDEX idx_deleted ON Item (DeletedFlag);")
            # cursor.execute("CREATE INDEX idx_ItemDocNbr ON Item (ItemDocumentNbr);")
            # cursor.execute("CREATE INDEX idx_CustName ON Customer (CustomerName);")
            cursor.execute("SELECT ItemId,ItemDocumentNbr,CreateDate,UpdateDate,CustomerID from Item where DeletedFlag = 0;")
            item_rows = cursor.fetchall()
            data = dict(ItemId=[], ItemDocumentNbr=[], CustomerName=[], CreateDate=[], UpdateDate=[],ItemSource=[])
            for row in item_rows:
                buscar = row[4]
                CreateDate = row[2].strftime("%Y-%m-%d %H:%M:%S")
                row[2] = CreateDate
                UpdateDate = row[3].strftime("%Y-%m-%d %H:%M:%S")
                row[3] = UpdateDate
                cursor.execute("SELECT CustomerName FROM Customer where CustomerID = {0};".format(buscar))
                search = cursor.fetchone()
                CustomerName = search
                row[4] = CustomerName
                ItemSource = CustomerName[0:2]
                if ItemSource == '99':
                    ItemSource = 'Local '
                else:
                    ItemSource = 'External'
                data['ItemId'].append(row[0])
                data['ItemDocumentNbr'].append(row[1])
                data['CustomerName'].append(row[4])
                data['CreateDate'].append(row[2])
                data['UpdateDate'].append(row[3])
                data['ItemSource'].append(ItemSource)
            pd.DataFrame(data=data)
            pd.to_csv('{0}.csv'.format(targetFile),sep =';',compression='gzip')
            # End of exercise
        except:
            print("error extracting data from sqlserver")
        finally:        
            if conn: conn.close()