import os
import numpy as np
import pandas as pd

import hydra
from omegaconf import DictConfig, OmegaConf



@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    data_dir = cfg.data
    
    master = pd.read_excel(data_dir.metadata)
    master.drop(columns=['Sample_body_site', 'Type', 'urls'], inplace=True)
    unique_master = master.drop_duplicates(subset=['Subject_ID', 'Visit_number', 'Visit_ID', 'Age', 'Sex', 'Diagnosis', 'Race'])
    
    tax_dir = data_dir.tax
    files = os.listdir(tax_dir)
    files = [os.path.join(tax_dir, f) for f in files]
    tax = pd.concat(map(pd.read_csv, files), ignore_index=True)

    genus_abundances = tax.loc[:,["Visit_name", 'Genus', 'Metaphlan2_Analysis']]
    genus_abundances.reset_index(inplace=True)
    genus_abundances.rename(columns={'index': 'idx', 'Visit_name': 'Visit_ID'}, inplace=True)
    genus = genus_abundances.pivot(index='idx', columns='Genus', values='Metaphlan2_Analysis')
    genus['Visit_ID'] = genus_abundances['Visit_ID']
    group_genus = genus.groupby('Visit_ID').sum()
    
    master_tax = pd.merge(unique_master, group_genus, on='Visit_ID')
    master_tax = master_tax.fillna(0.0)
    master_tax.to_csv(os.path.join(data_dir.agg, "master_tax.csv"))


if __name__ == "__main__":
    main()
