import argparse
import os, sys
from loguru import logger

import hydra
from omegaconf import DictConfig, OmegaConf

from config import NEO4J, DATA_DIR, NEO4J_IMPORT_DIR
from cypher_queries import QUERIES
from tqdm import tqdm

from neo4j import GraphDatabase

def read_queries(config: DictConfig):
    queries: dict[str, str] = {}
    logger.info("Fetching queries...")
    with open(os.path.join(config.queries, 'query_ecs_yi.txt'), "r") as file:
        queries["ecs"] = file.read()
        logger.info(f"ECS: {queries['ecs']}")
    with open(os.path.join(config.queries, 'query_pwy_yi.txt'), "r") as file:
        queries["pwy"] = file.read()
        logger.info(f"PWY: {queries['pwy']}")
    with open(os.path.join(config.queries, 'query_tax_yi.txt'), "r") as file:
        queries["tax"] = file.read()
        logger.info(f"TAX: {queries['tax']}")

    return queries

@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    config = {
        "handlers": [
            {
                "sink": sys.stdout,
                "format": "{time} - {name} {level} {message}",
                "level": "INFO",
                # "encoding": "utf-8"
            },
            {
                "sink": 'file_{time}.log',
                "serialize": True,
                "format": "{time} - {name} {level} {message}",
                "level": "INFO",
                "rotation": "8 MB",
                # "encoding": "utf-8"
            },
        ],
        "extra": {"user": "someone"}
    }
    logger.configure(**config)

    # logger.add('file_{time}.log', format="{name} {level} {message}", level="INFO", rotation="8 MB", encoding="utf-8")
    kg_config = cfg.kg
    
    files_list: list[dict[str, str | list]] = []
    configs = [
        [os.path.join(kg_config.input, 'ecs'), os.path.join(kg_config.queries, 'query_ecs_yi.txt'), 'ecs'],
        [os.path.join(kg_config.input, 'pwy'), os.path.join(kg_config.queries, 'query_pwy_yi.txt'), 'pwy'],
        [os.path.join(kg_config.input, 'tax'), os.path.join(kg_config.queries, 'query_tax_yi.txt'), 'tax'],
    ]
    
    for item in configs:
        if item[0] is not None and item[1] is not None:
            files_list.append({"name": item[2], "path": item[0], "files": []})
            logger.info(
                f"Loading {item[2]}-data from {item[0]} unsing query in {item[1]}."
            )
        else:
            logger.info(
                f"No folder or query was provided for {item[2]}-data. Skipping {item[2]} import. \n\t"
                f"Please note that a path must be provided for esc, pwy, tax data - the files must be at least "
                f"one folder below the neo4j import folder."
            )
    logger.info(f"files list: {files_list}")
    
    QUERIES = read_queries(kg_config)
    queries_store = []
    tmp_str = str(kg_config.neo4j.import_folder)
    logger.info(f"import_folder: {tmp_str}")
    neo4j_import_path = tmp_str[tmp_str.rfind('/import/') + len('/import/'):]
    logger.info(f"neo4j import path: {neo4j_import_path}")
    
    for file_item in files_list:
        logger.info(file_item)
        data_dir = os.path.join(str(kg_config.neo4j.import_folder), str(file_item["path"]))
        if os.path.exists(data_dir):
            file_item["files"] = os.listdir(data_dir)

            for file in file_item["files"]:
                queries_store.append(
                    [
                        QUERIES[file_item["name"]],
                        f"file:///{neo4j_import_path}/{file_item['path']}/{file}",
                        f"Importing {file_item['name']} data from {file}",
                    ]
                )
        else:
            logger.info(
                f"Unable to locate folder {file_item['path']}. {file_item['name']}-data will not be imported."
            )
    logger.info(f"queries store: {queries_store}")
    
    # sys.exit("quitting")
    logger.info("Executing queries:")
    # NEO4J = {"URL": "bolt://localhost:7687", "USER": "neo4j", "PW": "1qazXSW@", 'DB': 'ecspwytax'}
    URI = str(kg_config.neo4j.uri)
    logger.info(f"URI: {URI}")
    AUTH = (str(kg_config.neo4j.user), str(kg_config.neo4j.pw))
    logger.info(f"AUTH: {AUTH}")
    DB = str(kg_config.neo4j.db)
    logger.info(f"DB: {DB}")

    with tqdm(total=len(queries_store)) as pbar:
        with GraphDatabase.driver(URI, auth=AUTH) as driver: 
            driver.verify_connectivity()
            for query in queries_store:
                pbar.set_description(query[2])
                pbar.refresh()
                logger.info(f"processing {query[1]}")
                driver.execute_query(query[0], path=query[1], database_=DB)
                pbar.update()


if __name__ == "__main__":
    main()
