from dataclasses import Field
from datetime import datetime
import pandas as pd
from models import blockload as BlockLoad
import json
from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import psycopg2

app = FastAPI()

# Define Pydantic models

class ConditionBase(BaseModel):
    # id: Optional[int] = None
    field_name: str
    condition_type: str
    # comparison_field_name: Optional[str] = None
    value: str
    # days: Optional[int] = None
    # parameters: dict = {}
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

class ConditionCreate:
    pass

class RuleBase(BaseModel):
    name: str
    description: str

class RuleCreate(RuleBase):
    # meter_type: str
    # load_type: str
    # user_type:str
    condition: ConditionBase
    # actions: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    pass

class RuleGroupBase(BaseModel):
    name: str
    description: str

class RuleGroupCreate(RuleGroupBase):
    pass

class DataGroupRuleBase(BaseModel):
    data_type: str
    list_of_groups: List[int]
    list_of_rules: List[int]

class DataGroupRuleCreate(DataGroupRuleBase):
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

def get_data_groups_rules(type: str):
    query = f"SELECT * FROM data_type_mapping where data_type = '{type}';"
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    data = fetch_data(query, connection)
    data = data[0]
    if not data:
        raise HTTPException(status_code=404, detail="No groups found")
    print(data)
    result = {'data_type': data[0], 'list_of_groups': data[1], 'list_of_rules': data[2]}
    #json_string = json.dumps(result)
   
    return result

def get_rules_from_group_rule_mapper(groups: list):
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    rule_list = []
    for group in groups:
        query = f"SELECT * FROM rulegroupmapping where group_id  = '{group}';"
        data = fetch_data(query, connection)
        if not data:
            raise HTTPException(status_code=404, detail="No mappings found")
        result = [{'id': item[0], 'rule_id': item[1], 'group_id': item[2]} for item in data]
        for item in result:
            rule_list.append(item.get('rule_id'))
        
    return rule_list


class RuleNamesRequestModel(BaseModel):
    rule_names: List[str]

@app.post("/rules_by_rule_names/", response_model=List[RuleCreate])
def get_rules_by_rule_names(rules: RuleNamesRequestModel):
    connection = get_db_connection()
    cursor = connection.cursor()
    result = []
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    rule_name_tuple = tuple(rules.rule_names)
    query = f"SELECT * FROM rules where name in %s;"
    cursor.execute(query, (rule_name_tuple,))
    data = cursor.fetchall()
    result = []
    connection.commit()
    for item in data:
        conditions = get_conditions_for_rule(item[0])
        rule_data = {'id': item[0], 'name': item[1], 'description': item[5], 'condition': conditions[0]}
        result.append(rule_data)
    
    if not data:
        raise HTTPException(status_code=404, detail="No rule found")    
    return result  


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







# Activating and Deactivating Rules


@app.post("/rules_activation/")
def rules_activate(rules: RuleNamesRequestModel):
    connection = get_db_connection()
    cursor = connection.cursor()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    try:
        rule_name_tuple = tuple(rules.rule_names)
        query = f"UPDATE rules SET is_active = True where name in %s;"
        cursor.execute(query, (rule_name_tuple,))
        connection.commit()
    except:
         raise HTTPException(status_code=404, detail="Some Error Occured") 
    finally:
        if connection:
            connection.close()
    return {"message" : "Activated Successfully"}


@app.post("/rules_deactivation/")
def rules_deactivate(rules: RuleNamesRequestModel):
    connection = get_db_connection()
    cursor = connection.cursor()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    try:
        rule_name_tuple = tuple(rules.rule_names)
        query = f"UPDATE rules SET is_active = False where name in %s;"
        cursor.execute(query, (rule_name_tuple,))
        connection.commit()
    except:
         raise HTTPException(status_code=404, detail="Some Error Occured") 
    finally:
        if connection:
            connection.close()
        return {"message" : "Deactivated Successfully"}




# API endpoints to fetch data from the database

# Groups data fetch
@app.get("/groups/", response_model=List[RuleGroupCreate])
async def get_groups():
    query = "SELECT * FROM rule_groups;"
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


# Rules data fetch
@app.get("/rules/", response_model=List[RuleCreate])
async def get_rules():
    query = "SELECT * FROM rules;"
    connection = get_db_connection()
    cursor = connection.cursor()
    result = []
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor.execute(query)
    data = cursor.fetchall()
    connection.commit()
    print(data)
    for item in data:
        conditions = get_conditions_for_rule(item[0])
        rule_data = {'id': item[0], 'name': item[1], 'description': item[5], 'condition': conditions[0]}
        result.append(rule_data)
    if not data:
        raise HTTPException(status_code=404, detail="No rules found")
    print(result)
    return result


def get_conditions_for_rule(rule_id: int) -> List[dict]:
    conditions = []
    try:
        query = f"SELECT * FROM rules_conditions WHERE rule_id = {rule_id};"
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()

        for item in data:
            condition_data = {
                # 'id': item[0],
                'field_name': item[2],
                'condition_type': item[3],
                # 'comparison_field_name': item[4],
                'value': item[5],
                # 'days': item[6],
                # 'parameters': item[7],
                'created_at': item[8],
                'updated_at': item[9]
            }
            conditions.append(condition_data)

        return conditions

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        connection.close()





# API endpoints to create and delete groups
@app.post("/create_group/", response_model=RuleGroupCreate)
async def create_group(group: RuleGroupCreate):
    query = f"INSERT INTO rule_groups (name, description) VALUES ('{group.name}', '{group.description}');"
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

@app.delete("/groups/{group_name}/")
async def delete_group(group_name: str):
    query = f"DELETE FROM rulegroups WHERE name = {group_name};"
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
@app.post("/create_rule/", response_model=RuleCreate)
async def create_rule(rule: RuleCreate):
    query = f"INSERT INTO rules (name, description) VALUES ('{rule.name}', '{rule.description}') RETURNING id;"
    # Pre Cumulative Checks (field_name, condition_type and value is taken)
    # rule_condition = ConditionBase()
    rule_condition = rule.condition
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        rule_id = cursor.fetchone()[0]
        print(rule_id)
        query = f"INSERT INTO rules_conditions (field_name, condition_type, value, rule_id) VALUES ('{rule_condition.field_name}', '{rule_condition.condition_type}', '{rule_condition.value}', '{rule_id}');"
        cursor.execute(query)
        connection.commit()
        return rule
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error creating rule")
    finally:
        cursor.close()
        connection.close()

@app.delete("/rules/{rule_name}/")
async def delete_rule(rule_name: str):
    query = f"DELETE FROM rules WHERE name = '{rule_name}';"
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



# Adding rules to a group

@app.post("/add_rules_to_group/")
async def add_rules_to_group(group_name: str, rules_names: List[str]): # type: ignore
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try: 
        for rule_name in rules_names:
            query = f"SELECT id FROM rule_groups WHERE name = '{group_name}';"
            cursor.execute(query)
            id = cursor.fetchone()[0]
            print(id)
            query = f"UPDATE rules SET group_id = '{id}' WHERE name = '{rule_name}';"
            cursor.execute(query)
            connection.commit()
        return {"message": f"Rules added to group {group_name} successfully"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error adding rules")
    finally:
        cursor.close()
        connection.close()
    

@app.delete("/delete_rules_from_group/")
async def add_rules_to_group(group_name: str, rules_names: List[str]): # type: ignore
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try: 
        for rule_name in rules_names:
            query = f"SELECT id FROM rule_groups WHERE name = '{group_name}';"
            cursor.execute(query)
            id = cursor.fetchone()[0]
            print(id)
            query = f"UPDATE rules SET group_id = NULL WHERE name = '{rule_name}';"
            cursor.execute(query)
            connection.commit()
        return {"message": f"Rules removed from group {group_name} successfully"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error removing rules")
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
            # db->user type => List of groups and list of rules
            data_type_mapper = get_data_groups_rules('blockloadPhase1')
            print(data_type_mapper)
            # group_belongs = [3] # user(data) -> groups/rules
            # rule_belong = [4]

            rules_to_be_applied = []
            groups_list = data_type_mapper.get('list_of_groups')
            rules_list = data_type_mapper.get('list_of_rules')
            rules_from_groups = get_rules_from_group_rule_mapper(groups_list)
            
            for item in rules_list:
                rules_to_be_applied.append(item)
            for item in rules_from_groups:
                rules_to_be_applied.append(item)
            print(rules_to_be_applied)
            rule_ids_model = RuleIdsRequestModel()
            rule_ids_model.rule_ids = rules_to_be_applied
            rules_data = get_rules_by_rule_id(rules_to_be_applied)
            
            processed_data = processor.process_data(data, rules_to_be_applied, rules_data)
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