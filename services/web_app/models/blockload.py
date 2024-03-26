import pandas as pd

class DataProcessor:
    def read_from_csv(self, file):
        # Read data from CSV file
        return pd.read_csv(file)

    def process_data(self, data):
        # Placeholder method to be implemented by subclasses
        raise NotImplementedError("Subclasses must implement process_data method")

    def write_to_csv(self, data, output_file):
        # Write processed data to CSV file
        data.to_csv(output_file, index=False)

class BlockLoadPhase1(DataProcessor):
    def process_data(self, data):
        # Process block load data for phase 1 meter
        # Example processing: Add 10 to each value in the 'import_Wh' column
        data["Anomaly"] = "No"  # Assuming no anomaly initially
    
        # Example: Detect anomalies based on conditions
        # Replace this with your actual anomaly detection logic
    
        # For demonstration, let's assume an anomaly if the value in column 'import_VAh' is negative
        data.loc[data['import_VAh'] < 0, 'Anomaly'] = "import_VAh is negative"
        return data

class BlockLoadPhase3(DataProcessor):
    def process_data(self, data):
        # Process block load data for phase 3 meter
        # Example processing: Multiply values in the 'import_Wh' column by 2
        data['import_Wh'] *= 2
        return data

class BlockLoadLTCT(DataProcessor):
    def process_data(self, data):
        data['import_Wh'] *= 3
        return data
# Factory method to create instances of data processors
def create_data_processor(data_type):
    if data_type == 'BlockLoadPhase1':
        return BlockLoadPhase1()
    elif data_type == 'BlockLoadPhase3':
        return BlockLoadPhase3()
    elif data_type == 'BlockLoadLTCT':
        return BlockLoadLTCT();
    # Add more conditions for other data types

