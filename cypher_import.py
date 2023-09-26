import argparse
import os

from config import NEO4J, DATA_DIR, NEO4J_IMPORT_DIR
from cypher_queries import QUERIES
from tqdm import tqdm
from utils.neo4j_connection import Neo4jConnection


parser = argparse.ArgumentParser(
    description="Import CSV files in Neo4J. Input folders given the none argument will "
    "not be imported."
)
parser.add_argument(
    "-master_folder",
    dest="input_folder_master",
    default=f"{DATA_DIR}/data/csv/master",
    type=str,
    help="string: Location of cvs files for enzyme data (default: ../data/csv/ecs",
)
parser.add_argument(
    "-in_ecs",
    dest="input_folder_ecs",
    default=f"{DATA_DIR}/data/csv/ecs",
    type=str,
    help="string: Input folder with enzyme CSV files (is created if it does not exist, "
    "default: ./data/csv/ecs)",
)
parser.add_argument(
    "-in_pwy",
    dest="input_folder_pwy",
    default=f"{DATA_DIR}/data/csv/pwy",
    type=str,
    help="string: Input folder with enzyme CSV files (is created if it does not exist, "
    "default: ./data/csv/pwy)",
)
parser.add_argument(
    "-in_tax",
    dest="input_folder_tax",
    default=f"{DATA_DIR}/data/csv/tax",
    type=str,
    help="string: Input folder with enzyme CSV files (is created if it does not exist, "
    "default: ./data/csv/tax)",
)
ex_args = parser.parse_args()


def load_from_hhd():
    files_list = [
        {"name": "ecs", "path": f"{DATA_DIR}/data/csv/ecs", "files": []},
        {"name": "tax", "path": f"{DATA_DIR}/data/csv/tax", "files": []},
        {"name": "pwy", "path": f"{DATA_DIR}/data/csv/pwy", "files": []},
    ]

    conn = Neo4jConnection(NEO4J["URL"], NEO4J["USER"], NEO4J["PW"])

    queries_store = [
        [QUERIES["master_data"], "file:///master_list.csv", "Importing Master Data"]
    ]

    for item in files_list:
        if os.path.exists(item["path"]):
            item["files"] = os.listdir(item["path"])

            for file in item["files"]:
                queries_store.append(
                    [
                        QUERIES[item["name"]],
                        f"file:///{item['name']}/{file}",
                        f"Importing {item['name']} data from {file}",
                    ]
                )
        else:
            print(
                f"Unable to locate folder {item['path']}. {item['name']}-data will not be imported."
            )

    print("Executing queries:")
    with tqdm(total=len(queries_store)) as pbar:
        for query in queries_store:
            pbar.set_description(query[2])
            pbar.refresh()
            conn.query(query[0], path=query[1])

            pbar.update()

    conn.close()


if __name__ == "__main__":
    load_from_hhd()
