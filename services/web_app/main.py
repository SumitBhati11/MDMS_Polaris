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
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]

class RuleGroupBase(BaseModel):
    name: str
    description: str

class RuleGroupCreate(RuleGroupBase):
    pass

class HeadGroupBase(BaseModel):
    name: str
    description: str

class HeadGroupCreate(RuleGroupBase):
    meter_type: str
    load_type: str
    pass

# class DataGroupRuleBase(BaseModel):
#     data_type: str
#     list_of_groups: List[int]
#     list_of_rules: List[int]

# class DataGroupRuleCreate(DataGroupRuleBase):
#     pass


# Function to establish a database connection
def get_db_connection():
    try:
        connection = psycopg2.connect(
            dbname="validation_rules",
            user="postgres",
            password="Adgjmp@12",
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



#################################################################################

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
        rule_data = {'id': item[0], 'name': item[1], 'description': item[2], 'condition': conditions[0]}
        result.append(rule_data)
    
    if not data:
        raise HTTPException(status_code=404, detail="No rule found")    
    return result  



# Function to get rule data from rule_ids

def get_rules_by_rule_id(rule_ids):
    connection = get_db_connection()
    cursor = connection.cursor()
    result = []
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    rule_name_tuple = tuple(rule_ids)
    query = f"SELECT * FROM rules where id in %s;"
    cursor.execute(query, (rule_name_tuple,))
    data = cursor.fetchall()
    result = []
    connection.commit()
    for item in data:
        conditions = get_conditions_for_rule(item[0])
        rule_data = {'id': item[0], 'name': item[1], 'description': item[2], 'condition': conditions[0]}
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


# Rules data fetch
@app.get("/rules/", response_model=List[RuleCreate])
async def get_rules():
    query = f"SELECT * FROM rules;"
    connection = get_db_connection()
    cursor = connection.cursor()
    result = []
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor.execute(query)
    data = cursor.fetchall()
    connection.commit()
    for item in data:
        conditions = get_conditions_for_rule(item[0])
        print(conditions)
        rule_data = {'id': item[0], 'name': item[1], 'description': item[2], 'condition': conditions[0]}
        result.append(rule_data)
        print("RULE>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    if not data:
        raise HTTPException(status_code=404, detail="No rules found")
    return result


def get_conditions_for_rule(rule_id: int) -> List[dict]:
    conditions = []
    try:
        query = f"SELECT * FROM rulesconditions WHERE rule_id = {rule_id};"
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
                'created_at': item[11],
                'updated_at': item[12]
            }
            conditions.append(condition_data)

        return conditions

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        connection.close()





# API endpoints to create and delete groups
@app.post("/create_group/")
async def create_group(group: RuleGroupCreate):
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        query_check = f"SELECT * FROM rulegroups WHERE name = '{group.name}';"
        cursor = connection.cursor()
        cursor.execute(query_check)
        existing_group = cursor.fetchone()
        if existing_group:
            raise HTTPException(status_code=400, detail=f"A group with the name '{group.name}' already exists.")
        
        query = f"INSERT INTO rulegroups (name, description) VALUES ('{group.name}', '{group.description}');"

        cursor.execute(query)
        print("Done")
        connection.commit()
        print("Done2")
        print(f"Successfully Added Group: '{group.name}'")
        return {f"Successfully Added Group: '{group.name}'"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error creating group")
    finally:
        cursor.close()
        connection.close()

@app.delete("/groups/{group_name}/")
async def delete_group(group_name: str):
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try:
        query = f"SELECT id FROM rulegroups WHERE name = '{group_name}';"
        cursor.execute(query)
        id = cursor.fetchone()
        if not id:
            raise HTTPException(status_code=404, detail=f"Group: '{group_name}' not found.")
        query = f"DELETE FROM rulegroups WHERE name = '{group_name}';"
        cursor.execute(query)
        connection.commit()
        return {"message": f"Group: '{group_name}' deleted successfully"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error deleting group")
    finally:
        cursor.close()
        connection.close()





# API endpoints to create and delete groups
@app.post("/create_head_group/")
async def create_head_group(group: HeadGroupCreate):
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        query_check = f"SELECT * FROM headgroups WHERE group_name = '{group.name}';"
        print(query_check)
        cursor = connection.cursor()
        cursor.execute(query_check)
        existing_group = cursor.fetchone()
        if existing_group:
            raise HTTPException(status_code=400, detail=f"A group with the name '{group.name}' already exists.")
        
        query = f"INSERT INTO headgroups (group_name, description, meter_type, load_type) VALUES ('{group.name}', '{group.description}','{group.meter_type}','{group.load_type}');"

        cursor.execute(query)
        connection.commit()
        return {f"Successfully Added Head Group: '{group.name}' "}
    except psycopg2.Error as e:
        connection.rollback()
        print(e)
        raise HTTPException(status_code=500, detail="Error creating group")
    finally:
        cursor.close()
        connection.close()

@app.delete("/delete_head_group/{group_name}/")
async def delete_group(group_name: str):
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try:
        query = f"SELECT * FROM headgroups WHERE group_name = '{group_name}';"
        cursor.execute(query)
        id = cursor.fetchone()
        if not id:
            raise HTTPException(status_code=404, detail=f"Group: '{group_name}' not found.")
        query = f"DELETE FROM headgroups WHERE group_name = '{group_name}';"
        cursor.execute(query)
        connection.commit()
        return {"message": f"Group: '{group_name}' deleted successfully"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error deleting group")
    finally:
        cursor.close()
        connection.close()



# API endpoints to create and delete rules
@app.post("/create_rule/", response_model=RuleCreate)
async def create_rule(rule: RuleCreate):
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try:
        query_check = f"SELECT * FROM rules r JOIN rulesconditions rc ON r.id = rc.rule_id WHERE r.name = '{rule.name}' AND rc.field_name = '{rule.condition.field_name}' AND rc.condition_type = '{rule.condition.condition_type}' AND rc.value = '{rule.condition.value}';"
        cursor.execute(query_check)
        existing_rule = cursor.fetchone()
        if existing_rule:
            raise HTTPException(status_code=400, detail=f"A rule with the same name '{rule.name}' and condition parameters already exists.")
        query = f"INSERT INTO rules (name, description, is_active) VALUES ('{rule.name}', '{rule.description}','true') RETURNING id;"
        # Pre Cumulative Checks (field_name, condition_type and value is taken)
        # rule_condition = ConditionBase()
        rule_condition = rule.condition
        cursor.execute(query)
        rule_id = cursor.fetchone()[0]
        print(rule_id)
        query = f"INSERT INTO rulesconditions (field_name, condition_type, value, rule_id) VALUES ('{rule_condition.field_name}', '{rule_condition.condition_type}', '{rule_condition.value}', '{rule_id}');"
        cursor.execute(query)
        connection.commit()
        return rule
    except psycopg2.Error as e:
        connection.rollback()
        print(e)
        raise HTTPException(status_code=500, detail="Error creating rule")
    finally:
        cursor.close()
        connection.close()

@app.delete("/rules/{rule_name}/")
async def delete_rule(rule_name: str):
    
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try: 
        query = f"SELECT id FROM rules WHERE name = '{rule_name}';"
        cursor.execute(query)
        id = cursor.fetchone()
        if not id:
            raise HTTPException(status_code=404, detail=f"Rule: '{rule_name}' not found.")
        
        query = f"DELETE FROM rules WHERE name = '{rule_name}';"
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

@app.post("/add_rules_to_rule_group/")
async def add_rules_to_group(group_name: str, rules_names: List[str]): # type: ignore
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    query = f"SELECT id FROM rulegroups WHERE name = '{group_name}';"
    cursor.execute(query)
    group_id = cursor.fetchone()
    if not group_id:
        raise HTTPException(status_code=404, detail=f"Group: '{group_name}' not found.")
    try: 
        for rule_name in rules_names:
            query = f"SELECT id FROM rules WHERE name = '{rule_name}';"
            cursor.execute(query)
            id = cursor.fetchone()
            if not id:
                raise HTTPException(status_code=404, detail=f"Rule: '{rule_name}' not found. Note: No rules added.")
        for rule_name in rules_names:
            query = f"SELECT id FROM rules WHERE name = '{rule_name}';"
            cursor.execute(query)
            id = cursor.fetchone()[0]
            print(id)
            query = f"INSERT INTO rulegroupmapping (group_id, rule_id) VALUES ('{group_id[0]}','{id}');"
            cursor.execute(query)
            connection.commit()
        return {"message": f"Rules added to group {group_name} successfully"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error adding rules")
    finally:
        cursor.close()
        connection.close()
    

@app.delete("/delete_rules_from_rule_group/")
async def delete_rules_to_group(group_name: str, rules_names: List[str]): # type: ignore
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try: 
        for rule_name in rules_names:
            query = f"SELECT id FROM rulegroups WHERE name = '{group_name}';"
            cursor.execute(query)
            group_id = cursor.fetchone()
            if not id:
                raise HTTPException(status_code=404, detail=f"Group '{rule_name}' not found. Note: No rules deleted.")
            
            for rule in rules_names:
                query = f"SELECT id FROM rules WHERE name = '{rule}';"
                cursor.execute(query)
                rule_id = cursor.fetchone()[0]
                query = f"DELETE FROM rulegroupmapping WHERE rule_id = '{rule_id}' AND group_id = '{group_id[0]}';"
                cursor.execute(query)
                connection.commit()
        return {"message": f"Rules removed from group {group_name} successfully"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error removing rules")
    finally:
        cursor.close()
        connection.close()



@app.get("/fetch_head_groups/",response_model=List[RuleGroupCreate])
async def get_head_groups():
    query = "SELECT * FROM headgroups;"
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


@app.post("/add_rules_to_head_group/")
async def add_rules_to_head_group(group_name: str, rules_names: List[str]): # type: ignore
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    query = f"SELECT group_id FROM headgroups WHERE group_name = '{group_name}';"
    cursor.execute(query)
    group_id = cursor.fetchone()
    if not group_id:
        raise HTTPException(status_code=404, detail=f"Group: '{group_name}' not found.")
    try: 
        for rule_name in rules_names:
            query = f"SELECT id FROM rules WHERE name = '{rule_name}';"
            cursor.execute(query)
            id = cursor.fetchone()
            if not id:
                raise HTTPException(status_code=404, detail=f"Rule: '{rule_name}' not found. Note: No rules added.")
        for rule_name in rules_names:
            query = f"SELECT id FROM rules WHERE name = '{rule_name}';"
            cursor.execute(query)
            id = cursor.fetchone()[0]
            query = f"INSERT INTO headgroups_rules_mapping (head_group_id, rule_id) VALUES ('{group_id[0]}','{id}');"
            cursor.execute(query)
            connection.commit()
        return {"message": f"Rules added to head group {group_name} successfully"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error adding rules")
    finally:
        cursor.close()
        connection.close()
    

@app.delete("/delete_rules_from_head_group/")
async def delete_rules_to_head_group(group_name: str, rules_names: List[str]): # type: ignore
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    try: 
        for rule_name in rules_names:
            query = f"SELECT group_id FROM headgroups WHERE group_name = '{group_name}';"
            cursor.execute(query)
            group_id = cursor.fetchone()
            if not id:
                raise HTTPException(status_code=404, detail=f"Group '{rule_name}' not found. Note: No rules deleted.")
            
        for rule in rules_names:
            query = f"SELECT id FROM rules WHERE name = '{rule}';"
            cursor.execute(query)
            rule_id = cursor.fetchone()[0]
            query = f"DELETE FROM headgroups_rules_mapping WHERE rule_id = '{rule_id}' AND head_group_id = '{group_id[0]}';"
            cursor.execute(query)
            connection.commit()
        return {"message": f"Rules removed from head group {group_name} successfully"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error removing rules")
    finally:
        cursor.close()
        connection.close()


@app.post("/add_group_to_head_group/")
async def add_group_to_head_group(head_group_name: str, groups_names: List[str]): # type: ignore
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    query = f"SELECT group_id FROM headgroups WHERE group_name = '{head_group_name}';"
    cursor.execute(query)
    head_group_id = cursor.fetchone()
    if not head_group_id:
        raise HTTPException(status_code=404, detail=f"Group: '{head_group_name}' not found.")
    try: 
        for group_name in groups_names:
            query = f"SELECT id FROM rulegroups WHERE name = '{group_name}';"
            cursor.execute(query)
            id = cursor.fetchone()
            if not id:
                raise HTTPException(status_code=404, detail=f"Group: '{group_name}' not found. Note: no groups added.")
        for group_name in groups_names:
            query = f"SELECT id FROM rulegroups WHERE name = '{group_name}';"
            cursor.execute(query)
            group_id = cursor.fetchone()[0]
            query = f"INSERT INTO headgroups_rulegroups_mapping (head_group_id, rule_group_id) VALUES ('{head_group_id[0]}','{group_id}');"
            cursor.execute(query)
            connection.commit()
        return {"message": f"Groups added to group {head_group_name} successfully"}
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Error adding groups")
    finally:
        cursor.close()
        connection.close()
    

@app.delete("/delete_group_from_head_group/")
async def delete_group_from_head_group(head_group_name: str, group_names: List[str]): # type: ignore
    connection = get_db_connection()
    if not connection:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor()
    query = f"SELECT group_id FROM headgroups WHERE group_name = '{head_group_name}';"
    cursor.execute(query)
    head_group_id = cursor.fetchone()
    if not head_group_id:
        raise HTTPException(status_code=404, detail=f"Group: '{head_group_name}' not found.")
    try: 
        for group_name in group_names:
            query = f"SELECT id FROM rulegroups WHERE name = '{group_name}';"
            cursor.execute(query)
            head_group_id = cursor.fetchone()
            if not id:
                raise HTTPException(status_code=404, detail=f"Group '{group_name}' not found. Note: No groups deleted.")
            
        for group_name in group_names:
            query = f"SELECT id FROM rulegroups WHERE name = '{group_name}';"
            cursor.execute(query)
            group_id = cursor.fetchone()[0]
            print(group_id)
            query = f"DELETE FROM headgroups_rulegroups_mapping WHERE rule_group_id = '{group_id}' AND head_group_id = '{head_group_id[0]}';"
            cursor.execute(query)
            connection.commit()
        return {"message": f"Rules removed from group {head_group_name} successfully"}
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
        if meter_type == "Phase-1":
            processor = BlockLoad.create_data_processor('BlockLoadPhase1')
            data = processor.read_from_csv(file.file)
            connection = get_db_connection()
            if not connection:
                raise HTTPException(status_code=500, detail="Database connection error")
            cursor = connection.cursor()
            meter_type = "P-1"
            load_type = "BL"
            query = f"SELECT group_id FROM headgroups WHERE meter_type = '{meter_type}' and load_type = '{load_type}';"
            cursor.execute(query)
            head_group_id = cursor.fetchone()
            if not head_group_id:
                raise HTTPException(status_code=404, detail=f"Head Group not found.")
            
            head_group_id = head_group_id[0]

            print(head_group_id)

            # groups fetch 

            query = f"SELECT rule_group_id FROM headgroups_rulegroups_mapping WHERE head_group_id = '{head_group_id}';"
            cursor.execute(query)
            group_ids = cursor.fetchall()

            print(group_ids)

            # Extracting group IDs from the fetched result
            group_id_list = [str(group[0]) for group in group_ids]

            # Joining the IDs into a comma-separated string for the IN clause
            group_id_str = ','.join(group_id_list)

            # rules fetch from groups

            query = f"SELECT rule_id FROM rulegroupmapping WHERE group_id in ({group_id_str});"

            cursor.execute(query)
            rule_ids_from_rule_groups = cursor.fetchall()

            print(rule_ids_from_rule_groups)

            # Extracting group IDs from the fetched result
            rule_id_list = [str(rule[0]) for rule in rule_ids_from_rule_groups]

            # Joining the IDs into a comma-separated string for the IN clause
            #rule_id_str = ','.join(rule_id_list)






            # individual rules fetch


            query = f"SELECT rule_id FROM headgroups_rules_mapping WHERE head_group_id = '{head_group_id}';"
            cursor.execute(query)
            rule_ids_from_head_group = cursor.fetchall()

            print(rule_ids_from_head_group)

            # Extracting group IDs from the fetched result
            rule_id_list2 = [str(rule[0]) for rule in rule_ids_from_head_group]


             # add all the rules 
            rule_id_list.extend(rule_id_list2)


            rule_id_str = ','.join(rule_id_list)
            
            
            print(rule_id_str)
            # send them to evaluate


            # groups = await get_groups()
            # rules = await get_rules()
            # db->user type => List of groups and list of rules
            # data_type_mapper = get_data_groups_rules('blockloadPhase1')
            # group_belongs = [3] # user(data) -> groups/rules
            # rule_belong = [4]

            # groups_list = data_type_mapper.get('list_of_groups')
            # rules_list = data_type_mapper.get('list_of_rules')
            #  rules_from_groups = get_rules_from_group_rule_mapper(groups_list)
            
            # for item in rules_list:
            #     rules_to_be_applied.append(item)
            # for item in rules_from_groups:
            #     rules_to_be_applied.append(item)
            # print(rules_to_be_applied)
            # rule_ids_model = RuleIdsRequestModel()
            # rule_ids_model.rule_ids = rules_to_be_applied

            query = f"SELECT * FROM headgroups_rulegroups_mapping WHERE head_group_id = '{head_group_id}';"

            cursor.execute(query)
            groups_details = cursor.fetchall()
            rules_data = get_rules_by_rule_id(rule_id_list)
            
            processed_data = processor.process_data(data, rules_data)
            final_response = organize_meter_numbers_by_group(processed_data)
            final_response['total_size'] = data.shape[0]
            return final_response

        elif meter_type == "3-Phase":
            pass

        elif meter_type == "LT_HTCT":
            pass

        else:
            return {"Error": "Params not valid."}
    
    if load_type == "Instantaneous Profile":
        if meter_type == "Phase-1":
            processor = BlockLoad.create_data_processor('BlockLoadPhase1')
            data = processor.read_from_csv(file.file)
            connection = get_db_connection()
            if not connection:
                raise HTTPException(status_code=500, detail="Database connection error")
            cursor = connection.cursor()
            meter_type = "P-1"
            load_type = "IP"
            query = f"SELECT group_id FROM headgroups WHERE meter_type = '{meter_type}' and load_type = '{load_type}';"
            cursor.execute(query)
            head_group_id = cursor.fetchone()
            if not head_group_id:
                raise HTTPException(status_code=404, detail=f"Head Group not found.")
            
            head_group_id = head_group_id[0]

            print(head_group_id)

            # groups fetch 

            query = f"SELECT rule_group_id FROM headgroups_rulegroups_mapping WHERE head_group_id = '{head_group_id}';"
            cursor.execute(query)
            group_ids = cursor.fetchall()

            print(group_ids)

            # Extracting group IDs from the fetched result
            group_id_list = [str(group[0]) for group in group_ids]

            # Joining the IDs into a comma-separated string for the IN clause
            group_id_str = ','.join(group_id_list)

            # rules fetch from groups

            query = f"SELECT rule_id FROM rulegroupmapping WHERE group_id in ({group_id_str});"

            cursor.execute(query)
            rule_ids_from_rule_groups = cursor.fetchall()

            print(rule_ids_from_rule_groups)

            # Extracting group IDs from the fetched result
            rule_id_list = [str(rule[0]) for rule in rule_ids_from_rule_groups]

            # Joining the IDs into a comma-separated string for the IN clause
            #rule_id_str = ','.join(rule_id_list)






            # individual rules fetch


            query = f"SELECT rule_id FROM headgroups_rules_mapping WHERE head_group_id = '{head_group_id}';"
            cursor.execute(query)
            rule_ids_from_head_group = cursor.fetchall()

            print(rule_ids_from_head_group)

            # Extracting group IDs from the fetched result
            rule_id_list2 = [str(rule[0]) for rule in rule_ids_from_head_group]


             # add all the rules 
            rule_id_list.extend(rule_id_list2)


            rule_id_str = ','.join(rule_id_list)
            
            
            print(rule_id_str)
            # send them to evaluate


            # groups = await get_groups()
            # rules = await get_rules()
            # db->user type => List of groups and list of rules
            # data_type_mapper = get_data_groups_rules('blockloadPhase1')
            # group_belongs = [3] # user(data) -> groups/rules
            # rule_belong = [4]

            # groups_list = data_type_mapper.get('list_of_groups')
            # rules_list = data_type_mapper.get('list_of_rules')
            #  rules_from_groups = get_rules_from_group_rule_mapper(groups_list)
            
            # for item in rules_list:
            #     rules_to_be_applied.append(item)
            # for item in rules_from_groups:
            #     rules_to_be_applied.append(item)
            # print(rules_to_be_applied)
            # rule_ids_model = RuleIdsRequestModel()
            # rule_ids_model.rule_ids = rules_to_be_applied

            query = f"SELECT * FROM headgroups_rulegroups_mapping WHERE head_group_id = '{head_group_id}';"

            cursor.execute(query)
            groups_details = cursor.fetchall()
            rules_data = get_rules_by_rule_id(rule_id_list)
            
            processed_data = processor.process_data(data, rules_data)
            final_response = organize_meter_numbers_by_group(processed_data)
            final_response['total_size'] = data.shape[0]
            return final_response

    
    return {"modified_csv": "Done"}


# @app.on_event("startup")
# async def startup_event():
#     # Initialize the database when the application starts
#     initialize_database()



def organize_meter_numbers_by_group(rule_failure_dict):
    # Initialize a dictionary to store failed meter numbers grouped by group_id
    group_wise_failed_meter_numbers = {}
    connection = get_db_connection()
    cursor = connection.cursor()

    # Iterate over each rule_id in the rule_failure_dict
    for rule_id, failed_meter_ids in rule_failure_dict.items():
        # Find the group_id corresponding to the current rule_id
        query = f"SELECT group_id FROM rulegroupmapping WHERE rule_id ='{rule_id}';"
        cursor.execute(query)
        group_id = cursor.fetchone()
        group_id = group_id[0]
        print(group_id)
        query = f"SELECT name FROM rulegroups WHERE id ='{group_id}';"
        cursor.execute(query)

        group_name = cursor.fetchone()
        group_name = group_name[0]
        print(group_name)

        # Add failed_meter_ids to the corresponding group_id in the dictionary
        if group_name:
            if group_name not in group_wise_failed_meter_numbers:
                group_wise_failed_meter_numbers[group_name] = failed_meter_ids
            else:
                group_wise_failed_meter_numbers[group_name].extend(failed_meter_ids)
    print(group_wise_failed_meter_numbers)
    return group_wise_failed_meter_numbers

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)