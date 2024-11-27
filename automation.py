
import pandas as pd
import hashlib
from typing import Tuple

def process_payments(csv_file_path: str, chunk_size: int = 1000) -> pd.DataFrame:
    """Process physician payments with minimal output fields and sort by name."""
    aggregated_results = pd.DataFrame()
    
    for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size):
        physician_data = chunk[chunk['Physician_Profile_ID'].notna()]
        
        physician_data['Full_Name'] = physician_data.apply(
            lambda x: ' '.join(filter(None, [
                str(x['Physician_First_Name']).strip(),
                str(x['Physician_Middle_Name']).strip(),
                str(x['Physician_Last_Name']).strip()
            ])), axis=1)
        
        chunk_agg = physician_data.groupby('Physician_Profile_ID').agg({
            'Full_Name': 'first',
            'Total_Amount_of_Payment_USDollars': ['count', 'sum']
        })
        
        aggregated_results = pd.concat([aggregated_results, chunk_agg])
    
    final_results = aggregated_results.groupby(level=0).agg({
        ('Full_Name', 'first'): 'first',
        ('Total_Amount_of_Payment_USDollars', 'count'): 'sum',
        ('Total_Amount_of_Payment_USDollars', 'sum'): 'sum'
    }).reset_index()
    
    final_results.columns = [
        'Physician_Profile_ID', 'Full_Name', 'Number_of_Payments', 'Total_Amount'
    ]
    
    # Sort by Full_Name
    final_results = final_results.sort_values('Full_Name')
    
    return final_results

csv_file_path = 'research_payments.csv'
results = process_payments(csv_file_path)
results.to_csv('physician_payments_summary.csv', index=False)