import os
import numpy as np
import pandas as pd

DATA_DIR = './data/'
MASTER_FILE_NAME = 'Master_List.xlsx'
TAX_FILE_NAME = 'taxo_data_1.csv'
MASTER_TAX_FILE_NAME = 'master_tax.csv'

def master_tax():
    master_file = os.path.join(DATA_DIR, MASTER_FILE_NAME)
    tax_file = os.path.join(DATA_DIR, TAX_FILE_NAME)
    
    master = pd.read_excel(master_file)
    tax = pd.read_csv(tax_file)
    
    master.drop(columns=['Sample_body_site', 'Type', 'urls'], inplace=True)
    unique_master = master.drop_duplicates(
        subset=['Subject_ID', 'Visit_number', 'Visit_ID', 'Age', 'Sex', 'Diagnosis', 'Race']
    )
    genus_abundances = tax.loc[:,["Visit_name", 'Genus', 'Metaphlan2_Analysis']]
    genus_abundances.reset_index(inplace=True)
    genus_abundances.rename(columns={'index': 'idx', 'Visit_name': 'Visit_ID'}, inplace=True)
    genus = genus_abundances.pivot(index='idx', columns='Genus', values='Metaphlan2_Analysis')
    genus['Visit_ID'] = genus_abundances['Visit_ID']
    group_genus = genus.groupby('Visit_ID').sum()
    
    result = pd.merge(unique_master, group_genus, on='Visit_ID')
    result = result.fillna(0.0)
    result.to_csv(os.path.join(DATA_DIR, MASTER_TAX_FILE_NAME))



def main():
    master_tax()


if __name__ == "__main__":
    main()
