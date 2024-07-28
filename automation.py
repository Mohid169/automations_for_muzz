import pandas as pd

# Define the chunk size
chunk_size = 1000  # Adjust this based on your system's memory capacity

# Initialize an empty DataFrame to store the resultsß
aggregated_results = pd.DataFrame()

# Define the CSV file path
csv_file_path = 'research_payments.csv'  # Replace with the actual file path

# Iterate over the chunks
for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size):
    # Group by physician's full name and aggregate the number of payments and total amount
    chunk_agg = chunk.groupby(['Physician_First_Name', 'Physician_Middle_Name', 'Physician_Last_Name']).agg(
        Number_of_Payments=('Total_Amount_of_Payment_USDollars', 'size'),
        Total_Amount=('Total_Amount_of_Payment_USDollars', 'sum')
    ).reset_index()
    
    # Append the results to the main DataFrame
    aggregated_results = pd.concat([aggregated_results, chunk_agg])

# Group again to consolidate results from all chunks
final_results = aggregated_results.groupby(['Physician_First_Name', 'Physician_Middle_Name', 'Physician_Last_Name']).agg(
    Total_Number_of_Payments=('Number_of_Payments', 'sum'),
    Total_Amount_of_Payments=('Total_Amount', 'sum')
).reset_index()

# Save the results to a new CSV file
final_results.to_csv('physician_payments_summary_2013.csv', index=False)

print("Aggregation complete. Results saved to 'physician_payments_summary.csv'.")
