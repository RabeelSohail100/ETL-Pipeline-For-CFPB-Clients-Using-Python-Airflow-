import pandas as pd
from extract_data import extract  # Import extract function

def transform():
    extracted_data = extract()
# Convert list of dictionaries to DataFrame    
    df = pd.DataFrame(extracted_data)  

# Dropping unwanted columns
    drop_columns = [
        "complaint_what_happened", "date_sent_to_company", "zip_code", "tags",
        "has_narrative", "consumer_consent_provided", "consumer_disputed", "company_public_response"
    ]
    df.drop(columns=drop_columns, inplace=True, errors="ignore")

# Convert `date_received` to Month-Year
    df["date_received"] = pd.to_datetime(df["date_received"], errors="coerce")
    df["date_received"] = df["date_received"].dt.to_period("M").dt.to_timestamp("M")
    df["date_received"] = df["date_received"].dt.strftime('%Y-%m-%d')

# Group by required dimensions and count distinct complaints
    group_by_columns = [
        "product", "issue", "sub_product", "timely", "company_response",
        "submitted_via", "company", "date_received", "state", "sub_issue"
    ]
    df_transformed = df.groupby(group_by_columns, dropna=False).agg(
        complaint_count=("complaint_id", "nunique")
    ).reset_index()

    df_transformed.to_json('transformed_data.json', orient='records', lines=True)

    return df_transformed 

if __name__ == "__main__":
    transformed_df = transform()
    print(f"Transformed {len(transformed_df)} records.")
