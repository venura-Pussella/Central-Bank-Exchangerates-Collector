from src.operators.validation_cosmosdb import clean_and_validate_row

def convert_df_to_cosmos_db_format(df):
    cosmos_db_documents = []
    for _, row in df.iterrows():
        rate_document = clean_and_validate_row(row)
        cosmos_db_documents.append(rate_document)
    return cosmos_db_documents