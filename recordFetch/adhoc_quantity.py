import sys
sys.path.append("../")
from libraries.libraries import *
from connection.connection import dbConnection


class salesOrderFetch:
    def __init__(self):
        pass
    def fetchingTables():
        try:
            connection = dbConnection.connect()
            cursor = connection.cursor()
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            tables = cursor.fetchall()
            for table in tables:
                print(table.TABLE_NAME)
        except pyodbc.Error as err:
            print(f"Error: {err}")
        finally:
            connection.close()        
    def salesOrder():
        try:
            connection = dbConnection.connect()
            cursor = connection.cursor()
            cursor.execute("select * from IV") 
            columns = []
            
            for column in cursor.description:
                columns.append(column[0])
            
            table = cursor.fetchall()
            
            
            aggregated_records = defaultdict(lambda: defaultdict(lambda: {'Qty': 0, 'DebtorName': ''}))
            records = []
            
            for row in table:
                row_dict = dict(zip(columns, row))
                for month in range(1,13):
                    if row_dict.get('DocDate') and row_dict.get('DocDate').year == datetime.now().year and row_dict.get('DocDate').month == month:
                        cursor.execute("select * from IVDTL where DocKey = ?", row_dict["DocKey"])
                        
                        invoice_details_columns = []
                        
                        for column in cursor.description:
                            invoice_details_columns.append(column[0])
                            
                        record = cursor.fetchall()
                        
                        for data in record:
                            # print(data)
                            itemListing = dict(zip(invoice_details_columns, data))
                            if itemListing["ItemCode"] in ["5080P", "5080R", "5080RS", "6000P"]:
                                aggregated_records[itemListing["DocKey"]][itemListing["ItemCode"]]['Qty'] += itemListing["Qty"]
                                aggregated_records[itemListing["DocKey"]][itemListing["ItemCode"]]['DebtorName'] = row_dict["DebtorName"]
                               
                        for doc_key, items in aggregated_records.items():
                            for item_code, details in items.items():
                                records.append({
                                    "Invoice Number": row_dict["DocNo"],
                                    "DocKey": doc_key,
                                    "ItemCode": item_code,
                                    "TotalQty": details['Qty'],
                                    "DebtorName": details['DebtorName'],
                                    "UDF_Thickness": itemListing.get("UDF_Thickness"),
                                    "UDF_Width": itemListing.get("UDF_Width"),
                                    "UDF_Length": itemListing.get("UDF_Length"),
                                })
            
            for record in records:
                if record["TotalQty"] == 1:
                    print(record)
                    
        except pyodbc.Error as err:
            print(f"Error: {err}")

        finally:
            connection.close()
            
            
salesOrderFetch.salesOrder()