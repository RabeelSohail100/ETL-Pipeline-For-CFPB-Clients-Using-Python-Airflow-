import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# Load Transformed Data
def load_to_gsheet():
    # Authenticate with Google Sheets API
    creds = Credentials.from_service_account_file("cfpb_project/credentials/cfpb-data-project-68589d210efd.json", scopes=["https://www.googleapis.com/auth/spreadsheets"])
    client = gspread.authorize(creds)

    SPREADSHEET_ID = "1LttgLdaEeQ7t2zo8kHEWVrjmch6ed4S8LQAr8q1trXg"
    SHEET_NAME = "CFPB_ETL_Project_Data"

    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

# Read transformed JSON file
    df = pd.read_json("transformed_data.json", lines=True)

# Convert DataFrame to list of lists for Google Sheets
    data = [df.columns.tolist()] + df.values.tolist()

    sheet.clear()
    sheet.update(range_name="A1", values=data)

    print("Data successfully dump to Google Sheet!")
    
if __name__ == "__main__":
    load_to_gsheet()
