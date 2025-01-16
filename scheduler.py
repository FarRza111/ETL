import win32com.client
import win32com.client
import os
import pandas as pd
 
 
def refresh_excel_queries(file_path):
 
    excel_app = win32com.client.Dispatch("Excel.Application")
    excel_app.Visible = False  # Keeps Excel hidden
 
    try:
        workbook = excel_app.Workbooks.Open(file_path)
 
        # Refresh all connections
        print("Refreshing connections...")
        for connection in workbook.Connections:
            connection.Refresh()
         
        # Refresh all queries (Power Query)
        print("Refreshing queries...")
        workbook.RefreshAll()
        excel_app.CalculateUntilAsyncQueriesDone()
        workbook.Save()
        print("Workbook saved successfully!")
 
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        workbook.Close(SaveChanges=False)
        excel_app.Quit()
 
# if __name__ == "__main__":
 
file_path = r'C:Fariz/Local/mock.xlsx'
 
refresh_excel_queries(file_path)
