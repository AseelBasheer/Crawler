import pandas as pd
import requests
from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table, DateTime, Float, engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

def generate_create_table_statement(url, table_name):
    response = requests.get(url)
    data = response.json()

    if len(data) > 0:
        df = pd.DataFrame(data)

        column_definitions = []
        for column_name, column_type in zip(df.columns, df.dtypes):
            if "object" in str(column_type):
                column_type = String(length=255)
            elif "int" in str(column_type):
                column_type = Integer()
            elif "datetime" in str(column_type):
                column_type = DateTime()
            elif "float" in str(column_type):
                column_type = Float()
            else:
                column_type = String(length=255)

            column_definition = Column(column_name, column_type)
            column_definitions.append(column_definition)

        # Create the SQLAlchemy engine
        engine = create_engine('sqlite:///cdc_data.db', echo=True)  # 'cdc_data.db' database name

        # Define the SQLAlchemy table dynamically     
        metadata = MetaData(bind=engine)
        dynamic_table = Table(table_name, metadata, autoload=True, autoload_with=engine)

        # Create the table
        metadata.create_all()
        
        # Insert data into the table
        Session = sessionmaker(bind=engine)
        session = Session()

        for _, row in df.iterrows():
            data = dynamic_table.insert().values(**row)
            session.execute(data)

        session.commit()
        session.close()
        
        # Print the table metadata
        print(repr(dynamic_table))
        
        return dynamic_table
    else:
        print("No data found.")
        return None

# URL of the API
url = "https://data.cdc.gov/resource/k8wy-p9cg.json"
table_name = "cdc_data"

# Call the function to generate the CREATE TABLE statement, create the table, and insert data
dynamic_table = generate_create_table_statement(url, table_name)

# Print the CREATE TABLE statement
if dynamic_table:
    create_table_statement = str(dynamic_table.compile(dialect=engine.dialect))
    print(create_table_statement)