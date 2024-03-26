from fastapi import FastAPI, UploadFile, File
import pandas as pd
from models import blockload as BlockLoad
import json

import os
current_directory = os.getcwd()
print("Current directory:", current_directory)
app = FastAPI()


@app.get("/health-check/")
def health_check():
    return {"message": "OK"}

@app.post("/upload/")
async def upload_file(meter_type: str, load_type: str, file: UploadFile = File(...)):
    # Check file extension
    if not file.filename.endswith((".csv", ".xlsx")):
        return {"error": "Only CSV or Excel files are allowed."}

    if load_type == "Block Load":
        if meter_type == "1-Phase":
            processor = BlockLoad.create_data_processor('BlockLoadPhase1')
            data = processor.read_from_csv(file.file)
            processed_data = processor.process_data(data)
            processor.write_to_csv(processed_data, 'processed_block_load_data.csv')

        elif meter_type == "3-Phase":
            pass

        elif meter_type == "LT_HTCT":
            pass

        else:
            return {"Error": "Params not valid."}

    
    # Process the DataFrame to detect anomalies and add new columns
    # df = detect_anomalies(df)
    
    # Convert the DataFrame back to a CSV file
    # modified_csv = df.to_csv(index=False)
    # df.to_csv("modified_data.csv", index=True)
    
    return {"modified_csv": "Done"}
    # Perform further processing here, such as validation, parsing, etc.

def detect_anomalies(df):
    # Example: Add a new column to indicate anomalies
    df["Anomaly"] = "No"  # Assuming no anomaly initially
    
    # Example: Detect anomalies based on conditions
    # Replace this with your actual anomaly detection logic
    
    # For demonstration, let's assume an anomaly if the value in column 'import_VAh' is negative
    df.loc[df['import_VAh'] < 0, 'Anomaly'] = "import_VAh is negative"
    
    return df


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)