import os
import numpy as np
import pandas as pd

import hydra
from omegaconf import DictConfig, OmegaConf

def visit_stage(visit):
    if visit >= 4 and visit <=9:
        return "stage 1"
    elif visit >= 11 and visit <= 16:
        return "stage 2"
    elif visit >= 18 and visit <= 23:
        return "stage 3"
    elif visit >= 25 and visit <= 30:
        return "stage 4"
    else:
        return "unknown"

def age_group(age):
    if age <= 18:
        return 1
    elif age > 18 and age <= 40:
        return 2
    elif age > 40:
        return 3

def normalize_ecs(df):
    abd_sum = df.groupby(['Visit_name']).agg({'Abundance_RPKs': 'sum'})
    # print(abd_sum)
    result = df.merge(abd_sum[['Abundance_RPKs']], on='Visit_name', how='left')
    # print(result.columns)
    # result.drop('sum', axis=1, inplace=True)
    result.rename(columns={'Abundance_RPKs_x': 'Abundance_RPKs', 'Abundance_RPKs_y': 'sum'}, inplace=True)
    result['Abundance_normal'] = result['Abundance_RPKs'] / result['sum'] * 100.0
    result.drop(['Abundance_RPKs', 'sum'], axis=1, inplace=True)
    result.rename(columns={'Abundance_normal': 'Abundance_RPKs'}, inplace=True)
    return result

def normalize_pwy(df):
    abd_sum = df.groupby(['Visit_name']).agg({'Abundance': 'sum'})
    result = df.merge(abd_sum[['Abundance']], on='Visit_name', how='left')
    # result.drop('sum', axis=1, inplace=True)
    result.rename(columns={'Abundance_x': 'Abundance', 'Abundance_y': 'sum'}, inplace=True)
    result['Abundance_normal'] = result['Abundance'] / result['sum'] * 100.0
    result.drop(['Abundance', 'sum'], axis=1, inplace=True)
    result.rename(columns={'Abundance_normal': 'Abundance'}, inplace=True)
    return result

@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    data_dir = cfg.data

    master = pd.read_excel(data_dir.metadata)
    master.drop(columns=['Sample_body_site', 'Type', 'urls'], inplace=True)
    master = master.drop_duplicates(subset=['Subject_ID', 'Visit_number', 'Visit_ID', 'Age', 'Sex', 'Diagnosis', 'Race'])
    master.loc[:, ['age_group']] = master['Age'].apply(lambda age: age_group(age))
    master.loc[:, ['visit_stage']] = master['Visit_number'].apply(lambda visit: visit_stage(visit))
    print('master handled successfully.')

    dir = os.path.join(data_dir.genus_cleaned, 'ecs')
    files = os.listdir(dir)
    files = [os.path.join(dir, f) for f in files]
    df = pd.concat(map(pd.read_csv, files), ignore_index=True)
    normal = normalize_ecs(df)
    normal.rename(columns={'Visit_name': 'Visit_ID'}, inplace=True)
    normal.to_csv(os.path.join(data_dir.agg, 'ecs_normal.csv'), index=False)
    print('ecs normal saved.')
    master_ecs = pd.merge(master, normal, on='Visit_ID')
    master_ecs.to_csv(os.path.join(data_dir.agg, 'master_ecs.csv'), index=False)
    print('master ecs saved.')
    total = master_ecs.groupby(['Diagnosis', 'Genus', 'Enzyme'])['Abundance_RPKs'].agg(['mean', 'count', 'sum']).reset_index()
    total.to_csv(os.path.join(data_dir.agg, 'ecs_total.csv'), index=False)
    print('ecs total agg saved.')
    sex = master_ecs.groupby(['Diagnosis', 'Sex', 'Genus', 'Enzyme'])['Abundance_RPKs'].agg(['mean', 'count', 'sum']).reset_index()
    sex.to_csv(os.path.join(data_dir.agg, 'ecs_sex.csv'), index=False)
    print('ecs sex agg saved.')
    age = master_ecs.groupby(['Diagnosis', 'age_group', 'Genus', 'Enzyme'])['Abundance_RPKs'].agg(['mean', 'count', 'sum']).reset_index()
    age.to_csv(os.path.join(data_dir.agg, 'ecs_age.csv'), index=False)
    print('ecs age agg saved.')
    
    dir = os.path.join(data_dir.genus_cleaned, 'pwy')
    files = os.listdir(dir)
    files = [os.path.join(dir, f) for f in files]
    df = pd.concat(map(pd.read_csv, files), ignore_index=True)
    normal = normalize_pwy(df)
    normal.rename(columns={'Visit_name': 'Visit_ID'}, inplace=True)
    normal.to_csv(os.path.join(data_dir.agg, 'pwy_normal.csv'), index=False)
    print('pwy normal saved.')
    master_pwy = pd.merge(master, normal, on='Visit_ID')
    total = master_pwy.groupby(['Diagnosis', 'Genus', 'PWY'])['Abundance'].agg(['mean', 'count', 'sum']).reset_index()
    total.to_csv(os.path.join(data_dir.agg, 'pwy_total.csv'), index=False)
    print('pwy total agg saved.')
    sex = master_pwy.groupby(['Diagnosis', 'Sex', 'Genus', 'PWY'])['Abundance'].agg(['mean', 'count', 'sum']).reset_index()
    sex.to_csv(os.path.join(data_dir.agg, 'pwy_sex.csv'), index=False)
    print('pwy sex agg saved.')
    age = master_pwy.groupby(['Diagnosis', 'age_group', 'Genus', 'PWY'])['Abundance'].agg(['mean', 'count', 'sum']).reset_index()
    age.to_csv(os.path.join(data_dir.agg, 'pwy_age.csv'), index=False)
    print('pwy age agg saved.')
    visit = master_pwy.groupby(['Diagnosis', 'visit_stage', 'Genus', 'PWY'])['Abundance'].agg(['mean', 'count', 'sum']).reset_index()
    visit.to_csv(os.path.join(data_dir.agg, 'pwy_visit.csv'), index=False)
    print('pwy visit agg saved.')

    dir = os.path.join(data_dir.genus_cleaned, 'tax')
    files = os.listdir(dir)
    files = [os.path.join(dir, f) for f in files]
    tax = pd.concat(map(pd.read_csv, files), ignore_index=True)

    genus_abundances = tax.loc[:,["Visit_name", 'Genus', 'Metaphlan2_Analysis']]
    genus_abundances.reset_index(inplace=True)
    genus_abundances.rename(columns={'index': 'idx', 'Visit_name': 'Visit_ID'}, inplace=True)
    genus = genus_abundances.pivot(index='idx', columns='Genus', values='Metaphlan2_Analysis')
    genus['Visit_ID'] = genus_abundances['Visit_ID']
    group_genus = genus.groupby('Visit_ID').sum()
    master_tax = pd.merge(master, group_genus, on='Visit_ID')
    master_tax = master_tax.fillna(0.0)
    # master_tax.to_csv(os.path.join(data_dir.agg, "master_tax_cleaned.csv"))
    genus_columns = master_tax.columns[9:]
    outliers = []
    for col in genus_columns:
        outliers.append(master_tax[master_tax[col] > 100.0])
    outs = [out for out in outliers if len(out) > 0]
    result = pd.concat(outs)
    master_tax = master_tax[~master_tax.index.isin(result.index)]
    # print(master_tax.columns[:11])
    # master_tax.drop(master_tax.columns[[0]], axis=1, inplace=True)
    # print(master_tax.columns[:11])
    master_tax.to_csv(os.path.join(data_dir.agg, 'master_tax_normal.csv'), index=False)
    print('master tax normal saved.')
    # normal = cleaned_master_tax2.iloc[:, 7:].div(cleaned_master_tax2.iloc[:, 7:].sum(axis=1), axis=0)
    # cleaned_master_tax2.loc[:, ['sum_abundance']] = cleaned_master_tax2.filter(regex='^g_', axis=1).sum(axis=1)
    # normal.loc[:, 'sum_abundance'] = normal.filter(regex='^g_', axis=1).sum(axis=1)
    # normal = normal * 100.0
    # keys = ['Visit_name', 'Genus']
    # ecs_pwy = pd.merge(ecs, pwy, left_on=keys, right_on=keys)

    # ecs_index = ecs[~ecs.loc[:, 'Genus'].isin(genus)].index
    # ecs.drop(~ecs.loc[:, 'Genus'].isin(genus))
    # pwy_index = pwy[~pwy.loc[:, 'Genus'].isin(genus)].index
    # ecs.groupby(['Visit_name']).agg({'Abundance_RPKs': 'sum'})
    # ecs['Abundance_RPKs'].div(ecs.groupby(['Visit_name']).agg({'Abundance_RPKs': 'sum'}), axis=0)
    # print(master_tax.columns[:12])
    genus_columns = master_tax.columns[9:]
    total = master_tax.groupby(['Diagnosis'])[genus_columns].agg(['mean', 'count', 'sum']).reset_index()
    total.to_csv(os.path.join(data_dir.agg, 'tax_total.csv'), index=False)
    print('tax total agg saved.')
    sex = master_tax.groupby(['Diagnosis', 'Sex'])[genus_columns].agg(['mean', 'count', 'sum']).reset_index()
    sex.to_csv(os.path.join(data_dir.agg, 'tax_sex.csv'), index=False)
    print('tax sex agg saved.')
    age = master_tax.groupby(['Diagnosis', 'age_group'])[genus_columns].agg(['mean', 'count', 'sum']).reset_index()
    age.to_csv(os.path.join(data_dir.agg, 'tax_age.csv'), index=False)
    print('tax age agg saved.')
    visit = master_tax.groupby(['Diagnosis', 'visit_stage'])[genus_columns].agg(['mean', 'count', 'sum']).reset_index()
    visit.to_csv(os.path.join(data_dir.agg, 'tax_visit.csv'), index=False)
    print('tax visit agg saved.')

if __name__ == "__main__":
    main()
