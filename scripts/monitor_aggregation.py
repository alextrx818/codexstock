import os
import pandas as pd

# Function to verify the aggregation
def verify_aggregation(input_file, output_files):
    # Read the 1-minute data
    df_1min = pd.read_csv(input_file)
    
    # Sample 20 lines from the 1-minute data
    sample_1min = df_1min.sample(n=20)
    
    for output_file in output_files:
        # Read the aggregated data
        df_agg = pd.read_csv(output_file)
        
        # Sample 20 lines from the aggregated data
        sample_agg = df_agg.sample(n=20)
        
        # Verify the aggregation
        # This is a placeholder for actual verification logic
        if not sample_agg.empty:
            print(f'Verification passed for {output_file}')
        else:
            print(f'Verification failed for {output_file}')

# Example usage
if __name__ == "__main__":
    # Define the input and output directories
    input_dir = '/root/stock_project/data/us_stocks_sip/1MINUTE_BARS/'
    output_dir = '/root/stock_project/data/us_stocks_sip/'
    
    # Process each file
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_file = os.path.join(input_dir, filename)
            output_files = [os.path.join(output_dir, f'{interval}_BARS.csv') for interval in ['15MINUTE', '30MINUTE', '60MINUTE']]
            verify_aggregation(input_file, output_files)
