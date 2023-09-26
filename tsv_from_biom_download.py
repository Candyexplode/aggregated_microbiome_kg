#!/usr/bin/env python3
import argparse
import os
import re
import sys
from datetime import datetime
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from time import time

import biom
import keyboard
import pandas as pd
import requests
from tqdm import tqdm

parser = argparse.ArgumentParser(
    description="Download biom files specified in an Excel sheet and convert to TSV"
)
parser.add_argument("file_name", type=str, help="Excel file name with data source URLs")
parser.add_argument(
    "file_name2", type=str, help="Excel file name with data source URLs"
)
parser.add_argument(
    "-download_folder",
    type=str,
    default=".\\data\\biom",
    help="Define the download location for the biom files (default: .\\data\\biom)",
)
parser.add_argument(
    "-tsv_folder",
    default=".\\data\\tsv",
    type=str,
    help="Define the output location for the tsv files (default: .\\data\\tsv)",
)
parser.add_argument(
    "-url_filter",
    default="wgs",
    type=str,
    help="Define a string that must be part of a url for the script to download. URL where url_filter is not found, "
    "will not be downloaded! (default: wgs)",
)
parser.add_argument(
    "-non_parallel",
    action="store_true",
    help="Allow only serial downloads (not recommended)",
)
parser.add_argument(
    "-worker_processes",
    default=cpu_count() - 1,
    type=int,
    help="Max number of parallel downloads (default: CPU count - 1)",
)
ex_args = parser.parse_args()


def main():
    """
    Reands an Excel file with a 'url' column, downloads a biom file from each url, converts biom to TSV.
    """

    # Read file URLs from Excel
    if not os.path.exists(ex_args.file_name):
        sys.exit(
            "File {} does not exist in {}\\".format(
                ex_args.file_name, "\\".join(__file__.split("\\")[0:-1])
            )
        )

    file_manifest = pd.read_csv(ex_args.file_name, sep="\t")
    file_metadata = pd.read_csv(ex_args.file_name2, sep="\t")
    file_metadata.drop_duplicates(inplace=True)

    file_metadata.rename(columns={"study_full_name": "diagnosis"}, inplace=True)
    merged = file_manifest.merge(file_metadata, how="left", on="sample_id")

    url_list = list(merged["urls"])
    merged.to_csv("data\\master_data.csv")

    # Prepare urls and download destinations - replace " " in paths with "_" - handle duplicate files

    # Filter URLs
    url_list = [url for url in url_list if url.find(ex_args.url_filter) > -1]

    total_dl = len(url_list)
    path_list = []
    duplicates = 0
    for url in url_list:
        path = ex_args.download_folder + "\\" + url.split("/")[-1].replace(" ", "_")
        if path in path_list:
            duplicates += 1
            # Duplicate files are handled by appending "_DUPLICATE" to the filename.
            # Duplicate files will still be downloaded!
            path = path.replace(".biom", "_DUPLICATE.biom")
        path_list.append(path)

    if duplicates != 0:
        print(
            f"Warning: {duplicates} duplicate filenames found - renamed by appending '_DUPLICATE' to filename"
        )

    inputs = list(zip(url_list, path_list))

    if not os.path.exists(ex_args.download_folder):
        os.makedirs(ex_args.download_folder)  # create folder if it does not exist
    if not os.path.exists(ex_args.tsv_folder):
        os.makedirs(ex_args.tsv_folder)  # create folder if it does not exist

    # determine download mode ('in parallel' or not) and start downloading
    print("-------- " + str(datetime.now()) + " --------")

    t0 = time()

    if not ex_args.non_parallel:
        print(
            f"Downloading {total_dl} files found in {ex_args.file_name} in parallel using {ex_args.worker_processes} "
            f"workers (abort by holding 'ctrl + alt + q'):"
        )
        result = process_parallel(inputs, total_dl)
        if not result:
            print(f"Downloaded {total_dl} files. Total time: {time() - t0}")
        else:
            print("\nAborted by User.")
    else:
        print(
            f"Downloading {total_dl} files found in {ex_args.file_name} one by one "
            f"(abort by holding 'ctrl + alt + q'):"
        )
        user_abort = False
        with tqdm(total=total_dl) as pbar:
            for item in inputs:
                if keyboard.is_pressed("ctrl+alt+q"):
                    print("\nAborted by User.")
                    user_abort = True
                    break
                else:
                    process_url(item)
                    pbar.update()

            if not user_abort:
                print(f"Downloaded {total_dl} files. Total time: {time() - t0}")


def process_url(args):
    """
    Downloads biom files, converts them to tsv files and modifies them in preparation of Neo4J import.
    This function is called by pool.imap_unordered in case of parallel downloading and can thus only have one argument.
    Multiple inputs can be given to the function via iterables, in this case an args list.

    Arguments
    ---------
    args : list[url: str, filename: str]
        list with two items: location and filename (this function is called by pool.imap_unordered in case of parallel
        downloading and can thus only have one argument).
    """
    url, fn = args[0], args[1]
    t0 = time()
    try:
        r = requests.get(url)
        with open(fn, "wb") as f:
            f.write(r.content)
        download_duration = time() - t0
        t0 = time()

        biom_table = biom.load_table(fn)
        tsv_filename = fn.replace("biom", "tsv")

        with open(tsv_filename, "w") as f:
            biom_table.to_tsv(
                header_key="taxonomy", header_value="taxonomy", direct_io=f
            )

        # Modify tsv file to prepare it for neo4j loading
        with open(tsv_filename, "r") as fin:
            lines = fin.readlines()
            lines[1] = lines[1][1:].replace("OTU ID", "OTU_ID")
            lines[1] = re.sub("\t.*_Abundance", "\tAbundance", lines[1])
            lines[1] = lines[1].replace("-RPKs", "_RPKs")
            new_lines = [re.sub(".s__", "|s__", s) for s in lines]
        with open(tsv_filename, "w") as fout:
            fout.writelines(new_lines[1:])  # removes the first line before saving

        processing_duration = time() - t0

        return url, download_duration, processing_duration
    except Exception as e:
        print(f"\nException in download_url() for {url}:", e)


def process_parallel(args, total) -> bool:
    """
    Calls process_url to download biom files in parallel, convert them to tsv files and modify them in preparation of
    Neo4J import.

    Arguments
    ---------
    args : list[url: str, filename: str]
        list with two items: location and filename (the parameters for the process_url calls)
    total: int
        total number of urls to download (needed for the progress bar)
    """
    with ThreadPool(ex_args.worker_processes) as pool:
        results = pool.imap_unordered(process_url, args)
        i = 0
        with tqdm(total=total) as pbar:
            for _ in results:
                if keyboard.is_pressed("ctrl+alt+q"):
                    return True
                i += 1
                pbar.update()
        return False


if __name__ == "__main__":
    main()
