import argparse
import math
import os
import sys

import pandas as pd
from tqdm import tqdm

parser = argparse.ArgumentParser(
    description="Generate CSV files from prepared TSV files, ready for import in Neo4j"
)
parser.add_argument(
    "-in",
    dest="input_folder",
    default="./data/tsv",
    type=str,
    help="string: Location of TSV files to be processed (default: ./data/tsv)",
)
parser.add_argument(
    "-out_ecs",
    dest="output_folder_ecs",
    default="./data/csv/ecs",
    type=str,
    help="string: Output folder to store the enzyme CSV files in (is created if it does not exist, "
    "default: ./data/csv/ecs)",
)
parser.add_argument(
    "-out_pwy",
    dest="output_folder_pwy",
    default="./data/csv/pwy",
    type=str,
    help="string: Output folder to store the enzyme CSV files in (is created if it does not exist, "
    "default: ./data/csv/pwy)",
)
parser.add_argument(
    "-out_tax",
    dest="output_folder_tax",
    default="./data/csv/tax",
    type=str,
    help="string: Output folder to store the enzyme CSV files in (is created if it does not exist, "
    "default: ./data/csv/tax)",
)
parser.add_argument(
    "-pwy_filename",
    type=str,
    default="pwy_data.csv",
    help="string: Name of pathway CSV file to be created (default: pwy_data.csv",
)
parser.add_argument(
    "-process_pwy",
    default=1,
    type=int,
    choices=[0, 1],
    help="integer: Enables (1) or disables (0) processing of pathway files (default: 1)",
)
parser.add_argument(
    "-ecs_filename",
    type=str,
    default="ecs_data.csv",
    help="string: Name of enzyme CSV file to be created (default: ecs_data.csv",
)
parser.add_argument(
    "-process_ecs",
    default=1,
    type=int,
    choices=[0, 1],
    help="integer: Enables (1) or disables (0) processing of enzyme files (default: 1)",
)
parser.add_argument(
    "-taxo_filename",
    type=str,
    default="taxo_data.csv",
    help="string: Name of taxonomy CSV file to be created (default: taxo_data.csv",
)
parser.add_argument(
    "-process_taxo",
    default=1,
    type=int,
    choices=[0, 1],
    help="integer: Enables (1) or disables (0) processing of taxonomy files (default: 1)",
)
parser.add_argument(
    "-no_aggregation",
    action="store_true",
    help="Use this flag to output non-aggregated data (otherwise, aggregation on Visit_id, PWY/Enzyme and Genus - "
    "no aggregation on taxonomy data)",
)
ex_args = parser.parse_args()


def main():
    """
    Processes TSV files to CSV files for import in Neo4J. Use CLI arguments to set which files are to be processed.
    """
    if os.path.exists(ex_args.input_folder):
        files = os.listdir(ex_args.input_folder)
    else:
        sys.exit(f"Input folder can not be found: {ex_args.input_folder}")

    pwyfiles = pd.DataFrame()
    ecsfiles = pd.DataFrame()
    taxfiles = pd.DataFrame()

    # read all required files into one dataframe per experiment (pathway, enzyme, taxonomy)
    with tqdm(total=len(files)) as pbar:
        for file in files:
            if file.find("path") > -1 and ex_args.process_pwy == 1:
                df = process_pathway(f"{ex_args.input_folder}/{file}")
                pwyfiles = pd.concat([pwyfiles, df])
            elif file.find("ecs") > -1 and ex_args.process_ecs == 1:
                df = process_ecs(f"{ex_args.input_folder}/{file}")
                ecsfiles = pd.concat([ecsfiles, df])
            elif file.find("gene") > -1:
                pass  # not converted currently
            elif file.find("taxon") > -1 and ex_args.process_taxo == 1:
                df = process_taxo(f"{ex_args.input_folder}/{file}")
                taxfiles = pd.concat([taxfiles, df])

            temp_str = file
            while len(temp_str) < 50:
                temp_str += " "
            pbar.set_description(f"Processing {temp_str}")
            pbar.update()

    # for each experiment type (or depending on CLI args), call function check_and_split
    if ex_args.process_pwy == 1:
        # create folder if it does not exist
        if not os.path.exists(ex_args.output_folder_pwy):
            os.makedirs(ex_args.output_folder_pwy)
        if not ex_args.no_aggregation:
            pwyfiles = agg_sum(pwyfiles, "Abundance", ["Visit_name", "PWY", "Genus"])
        check_and_split(pwyfiles, ex_args.output_folder_pwy, ex_args.pwy_filename)

    if ex_args.process_ecs == 1:
        # create folder if it does not exist
        if not os.path.exists(ex_args.output_folder_ecs):
            os.makedirs(ex_args.output_folder_ecs)
        if not ex_args.no_aggregation:
            ecsfiles = agg_sum(
                ecsfiles, "Abundance_RPKs", ["Visit_name", "Enzyme", "Genus"]
            )
        check_and_split(ecsfiles, ex_args.output_folder_ecs, ex_args.ecs_filename)

    if ex_args.process_taxo == 1:
        # create folder if it does not exist
        if not os.path.exists(ex_args.output_folder_tax):
            os.makedirs(ex_args.output_folder_tax)
        check_and_split(taxfiles, ex_args.output_folder_tax, ex_args.taxo_filename)


def check_and_split(df: pd.DataFrame, out_folder: str, filename: str):
    """
    Splits dataframes with more than 5000 lines into separate dataframes and saves them as separate CSV files.

    Arguments
    ---------
    df : DataFrame
        Pandas DataFrame object to check, split and store on hard drive.
    out_folder: str
        Path to save CSV file(s) in.
    filename: str
        Filename to save CSV file(s) in. Appedned with "_i.csv" when a dataframe requires splitting, where i is a
        serial number.
    """
    max_length = 50000
    df_list = []
    df_length = df.shape[0]
    if df_length > max_length:
        for i in range(math.ceil(df_length / max_length)):
            df_list.append(df.iloc[i * max_length : (i + 1) * max_length])
    else:
        df_list.append(df)

    with tqdm(total=len(df_list)) as pbar:
        for i, df_i in enumerate(df_list):
            filename_i = filename.replace(".csv", f"_{i+1}.csv")
            df_i.to_csv(out_folder + "/" + filename_i)
            temp_str = filename
            while len(temp_str) < 40:
                temp_str += " "
            pbar.set_description(f"Splitting and storing {temp_str}")
            pbar.update()


def process_pathway(file: str) -> pd.DataFrame:
    df = pd.read_table(file)
    df["Visit_name"] = file.split("/")[-1][0:8]
    df["PWY"] = df["OTU_ID"].str.split(":").str[0]
    df["Genus"] = df["OTU_ID"].str.split("|").str[1]
    # df.drop(df[df["Genus"].isnull()].index, inplace=True)
    df.drop(df[df["Genus"] == "unclassified"].index, inplace=True)
    # df.drop(df[df["Abundance"] == 0].index, inplace=True)
    df.drop("OTU_ID", axis=1, inplace=True)
    if "taxonomy" in df.columns:
        df.drop("taxonomy", axis=1, inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def agg_sum(df, sum_colname, id_colnames):
    df["id"] = ""
    df["Genus"] = df["Genus"].fillna("")
    for colname in id_colnames:
        df["id"] += df[colname]
        if colname != id_colnames[-1]:
            df["id"] += "|"

    grouped = df.groupby(["id"])
    result = grouped.agg({sum_colname: "sum"})
    result = result.reset_index()

    for i, colname in enumerate(id_colnames):
        result[colname] = result["id"].str.split("|").str[i]

    # result.drop("id", axis=1, inplace=True)
    return result


def process_ecs(file: str) -> pd.DataFrame:
    df = pd.read_table(file)
    df["Visit_name"] = file.split("/")[-1][0:8]
    df.rename(columns={df.columns[1]: "Abundance_RPKs"}, inplace=True)
    df["Enzyme"] = df["OTU_ID"].str.split(":").str[0].str.split("|").str[0]
    df["Genus"] = df["OTU_ID"].str.split("|").str[1]
    df["Genus"] = df["Genus"].str.split(".").str[0]
    # df.drop(df[df["Genus"].isnull()].index, inplace=True)
    df.drop(df[df["Genus"] == "unclassified"].index, inplace=True)
    # df.drop(df[df["Abundance_RPKs"] == 0].index, inplace=True)
    df.drop("OTU_ID", axis=1, inplace=True)
    if "taxonomy" in df.columns:
        df.drop("taxonomy", axis=1, inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def process_taxo(file: str) -> pd.DataFrame:
    df = pd.read_table(file)
    df["Visit_name"] = file.split("/")[-1][0:8]
    df["Kingdom"] = df["OTU_ID"].str.split("|").str[0]
    df["Phylum"] = df["OTU_ID"].str.split("|").str[1]
    df["Class"] = df["OTU_ID"].str.split("|").str[2]
    df["Order"] = df["OTU_ID"].str.split("|").str[3]
    df["Family"] = df["OTU_ID"].str.split("|").str[4]
    df["Genus"] = df["OTU_ID"].str.split("|").str[5]
    df["Species"] = df["OTU_ID"].str.split("|").str[6]
    df.drop(df[df["Metaphlan2_Analysis"] == 0].index, inplace=True)
    df.drop(df[df["OTU_ID"].str.contains("t__")].index, inplace=True)
    df.drop(df[df["Genus"].isna()].index, inplace=True)
    df.drop(df[df["Species"].notna()].index, inplace=True)
    if "taxonomy" in df.columns:
        df.drop("taxonomy", axis=1, inplace=True)
    df.fillna("", inplace=True)
    return df


if __name__ == "__main__":
    main()
