import pandas as pd
from models import blockload as BlockLoad
import json
from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import psycopg2

app = FastAPI()

# Define Pydantic models
class RuleBase(BaseModel):
    name: str
    description: str
    conditions: Optional[str]
    actions: Optional[str]

class RuleCreate(RuleBase):
    pass

class RuleGroupBase(BaseModel):
    name: str
    description: str

class RuleGroupCreate(RuleGroupBase):
    pass

class RuleGroupMappingBase(BaseModel):
    rule_id: int
    group_id: int

class RuleGroupMappingCreate(RuleGroupMappingBase):
    pass

# Function to establish a database connection
def get_db_connection():
    try:
        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="",
            host="localhost",
            port="5432"
        )
        return connection
    except psycopg2.Error as e:
        print("Error: Unable to connect to the database.")
        print(e)
        return None

# Function to fetch data from the database
def fetch_data(query, connection):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        return data
    except psycopg2.Error as e:
        print("Error: Unable to fetch data from the database.")
        print(e)
        return None
    finally:
        if connection:
            connection.close()

# API endpoints to fetch data from the database
@app.get("/groups/", response_model=List[RuleGroupCreate])
async def get_groups():
    query = "SELECT * FROM rulegroups;"
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    data = fetch_data(query, connection)
    if not data:
        raise HTTPException(status_code=404, detail="No groups found")
    print(data)
    result = [{'id': item[0], 'name': item[1], 'description': item[2]} for item in data]
    #json_string = json.dumps(result)
   
    return result

@app.get("/rules/", response_model=List[RuleCreate])
async def get_rules():
    query = "SELECT * FROM rules;"
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    data = fetch_data(query, connection)
    print(data)
    if not data:
        raise HTTPException(status_code=404, detail="No rules found")
    result = [{'id': item[0], 'name': item[1], 'description': item[2], 'conditions': item[3], 'actions': item[4]} for item in data]
    print(result)
    return result

@app.get("/mappings/", response_model=List[RuleGroupMappingCreate])
async def get_mappings():
    query = "SELECT * FROM rulegroupmapping;"
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    data = fetch_data(query, connection)
    if not data:
        raise HTTPException(status_code=404, detail="No mappings found")
    result = [{'id': item[0], 'rule_id': item[1], 'group_id': item[2]} for item in data]
    return result

# API endpoints to create and delete groups
@app.post("/groups/", response_model=RuleGroupCreate)
async def create_group(group: RuleGroupCreate):
    query = f"INSERT INTO rulegroups (name, description) VALUES ('{group.name}', '{group.description}');"
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        return group
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error creating group")
    finally:
        cursor.close()
        connection.close()

@app.delete("/groups/{group_id}/")
async def delete_group(group_id: int):
    query = f"DELETE FROM rulegroups WHERE id = {group_id};"
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        return {"message": "Group deleted successfully"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error deleting group")
    finally:
        cursor.close()
        connection.close()

# API endpoints to create and delete rules
@app.post("/rules/", response_model=RuleCreate)
async def create_rule(rule: RuleCreate):
    query = f"INSERT INTO rules (name, description, conditions, actions) VALUES ('{rule.name}', '{rule.description}', '{rule.conditions}', '{rule.actions}');"
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        return rule
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error creating rule")
    finally:
        cursor.close()
        connection.close()

@app.delete("/rules/{rule_id}/")
async def delete_rule(rule_id: int):
    query = f"DELETE FROM rules WHERE id = {rule_id};"
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        return {"message": "Rule deleted successfully"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error deleting rule")
    finally:
        cursor.close()
        connection.close()

@app.get("/db-check/")
async def read_root():
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="",
        host="localhost",
        port="5432"
    )
    # Execute a sample query
    cur = conn.cursor()
    cur.execute("SELECT version();")
    db_version = cur.fetchone()[0]
    # Close database connection
    cur.close()
    conn.close()
    return {"message": "PostgreSQL version: " + db_version}

@app.get("/health-check/")
def health_check():
    return {"message": "OK"}

@app.post("/upload/")
async def upload_file(meter_type: str, load_type: str, file: UploadFile = File(...)):
    # Check file extension
    if not file.filename.endswith((".csv")):
        return {"error": "Only CSV are allowed."}

    if load_type == "Block Load":
        if meter_type == "1-Phase":
            processor = BlockLoad.create_data_processor('BlockLoadPhase1')
            data = processor.read_from_csv(file.file)
            groups = await get_groups()
            rules = await get_rules()
            mappings = await get_mappings()
            group_belongs = [3]
            rule_belong = [4]
            rules_to_be_applied = []
            for item in mappings:
                if item['group_id'] == group_belongs[0]:
                    rules_to_be_applied.append(item['rule_id'])
            for item in rule_belong:
                rules_to_be_applied.append(item)
            processed_data = processor.process_data(data, rules_to_be_applied, rules)
            processor.write_to_csv(processed_data, 'processed_block_load_data.csv')

        elif meter_type == "3-Phase":
            pass

        elif meter_type == "LT_HTCT":
            pass

        else:
            return {"Error": "Params not valid."}
        

    
    return {"modified_csv": "Done"}


# @app.on_event("startup")
# async def startup_event():
#     # Initialize the database when the application starts
#     initialize_database()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)