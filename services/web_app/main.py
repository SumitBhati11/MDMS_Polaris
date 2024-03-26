from fastapi import FastAPI, UploadFile, File
import pandas as pd

app = FastAPI()


@app.get("/health-check/")
def health_check():
    return {"message": "OK"}

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    # Check file extension
    if not file.filename.endswith((".csv", ".xlsx")):
        return {"error": "Only CSV or Excel files are allowed."}

    df = pd.read_csv(file.file)
    
    # Process the DataFrame to detect anomalies and add new columns
    df = detect_anomalies(df)
    
    # Convert the DataFrame back to a CSV file
    modified_csv = df.to_csv(index=False)
    df.to_csv("modified_data.csv", index=True)
    
    return {"modified_csv": modified_csv}
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