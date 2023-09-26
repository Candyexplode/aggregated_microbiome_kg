import os
import shutil

import numpy as np
import pandas as pd

import hydra
from omegaconf import DictConfig, OmegaConf

def get_genus_above_10(df):
    genus_counts_sum = df.groupby("Subject_ID")[df.columns[8:]]\
        .apply(lambda x: (x > 0).sum())\
        .applymap(lambda x: 1 if x > 0 else 0)\
        .sum()
    return [item for item, value in genus_counts_sum.items() if value > 10]

def change_name(file_path):
    # dir_name = os.path.dirname(file_path)
    filenames = os.path.splitext(os.path.basename(file_path))
    file_name = filenames[0]
    file_name += f'_genus_cleaned{filenames[1]}'
    # ext = os.path.splitext(os.path.basename(file_path))[1] #[1:]
    return file_name

def clean(file_path, genus_columns, new_dir): # genus_columns are the columns to keeped!
    df = pd.read_csv(file_path)
    cleaned = df[df['Genus'].isin(genus_columns)]
    new_file_name = change_name(file_path)
    cleaned.to_csv(os.path.join(new_dir, new_file_name))
    print(f"File {file_path} has been cleaned!")

@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    data_dir = cfg.data

    out_dirs = [
        os.path.join(data_dir.genus_cleaned, "ecs"),
        os.path.join(data_dir.genus_cleaned, "pwy"),
        os.path.join(data_dir.genus_cleaned, "tax")
    ]

    for dir in out_dirs:
        shutil.rmtree(dir)
        os.makedirs(dir)

    files_list = [
        {
            "name": "ecs",
            "path": data_dir.ecs,
            "files": os.listdir(data_dir.ecs),
            "out_path": out_dirs[0],
        },
        {
            "name": "pwy",
            "path": data_dir.pwy,
            "files": os.listdir(data_dir.pwy),
            "out_path": out_dirs[1],
        },
        {
            "name": "tax",
            "path": data_dir.tax,
            "files": os.listdir(data_dir.tax),
            "out_path": out_dirs[2],
        },
    ]
    master_tax = pd.read_csv(os.path.join(data_dir.agg, 'master_tax.csv'))
    genus_above_10 = get_genus_above_10(master_tax)
    for item in files_list:
        for f in item['files']:
            file_path = os.path.join(item['path'], f)
            clean(file_path=file_path, genus_columns=genus_above_10, new_dir=item['out_path'])


if __name__ == "__main__":
    main()
