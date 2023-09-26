import os
import numpy as np
import pandas as pd

import hydra
from omegaconf import DictConfig, OmegaConf



@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    data_dir = cfg.data
    
    master = pd.read_excel(data_dir.metadata)
    master_no_dup = master.drop_duplicates(subset=['Subject_ID', 'Visit_number', 'Visit_ID', 'Age', 'Sex', 'Diagnosis', 'Race'])
    master_no_dup.drop(columns=['Sample_body_site', 'Type', 'urls'], inplace=True)
    
    # ecs_dir = data_dir.ecs
    # pwy_dir = data_dir.pwy
    tax_dir = data_dir.tax

    files = os.listdir(tax_dir)
    files = [os.path.join(tax_dir, f) for f in files]
    df = pd.concat(map(pd.read_csv, files), ignore_index=True)

    genus_abundances = df.loc[:,["Visit_name", 'Genus', 'Metaphlan2_Analysis']]
    genus_abundances.rename(columns={'Visit_name': 'Visit_ID'}, inplace=True)
    genus_abundances.reset_index(inplace=True)
    genus_abundances.rename(columns={'index': 'idx'}, inplace=True)
    genus = genus_abundances.pivot(index='idx', columns='Genus', values='Metaphlan2_Analysis')
    genus['Visit_ID'] = genus_abundances['Visit_ID']
    
    df_master_tax = pd.merge(master_no_dup, genus, on='Visit_ID')
    df_master_tax = df_master_tax.fillna(0.0)
    df_master_tax.to_csv(os.path.join(data_dir.agg, "master_tax.csv"))


if __name__ == "__main__":
    main()
