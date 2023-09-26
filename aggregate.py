import operator
import os
from typing import Any, Callable

import hydra
import pandas as pd
from omegaconf import DictConfig, OmegaConf

OPERATORS = {
    "lt": operator.lt,
    "le": operator.le,
    "eq": operator.eq,
    "ne": operator.ne,
    "ge": operator.ge,
    "gt": operator.gt,
}

def aggregate(
    path: str,
    file_list: dict,
    df_meta: pd.DataFrame,
    agg_colname: str,
    id_colnames: Any,
    agg_func: list[str],
    modifications: Any,
) -> pd.DataFrame:
    li = []

    for filename in file_list:
        df = pd.read_csv(path + "/" + filename, index_col=False, header=0)
        li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)
    df["Genus"] = df["Genus"].fillna("")
    df.drop("Unnamed: 0", axis=1, inplace=True)

    merged = df.merge(df_meta, how="left", left_on="Visit_name", right_on="Visit_ID")
    merged.to_csv("./data/csv/test.csv")

    # do grouping
    if isinstance(modifications["grouping"], list):
        merged = group_dataframe(merged, modifications["grouping"])

    # apply coefficients
    if isinstance(modifications["agg_coefficients"], list):
        for rule in modifications["agg_coefficients"]:
            op = OPERATORS[rule["comparison"]]
            merged[agg_colname] = merged.apply(
                lambda x: apply_coefficient(
                    x[rule["property"]],
                    op,
                    rule["value"],
                    rule["coefficient"],
                    x[agg_colname],
                ),
                axis=1,
            )

    merged.to_csv("./data/csv/test.csv")
    column_names = list(merged.columns.values)
    for column in id_colnames:
        if column not in column_names:
            raise ValueError(
                f'The column "{column}" is not available in one of the files under {path}. '
                f"Make sure all defined columns are available after grouping."
            )

    result = merged.groupby(id_colnames).agg({agg_colname: agg_func})

    result = result.reset_index()

    return result

def apply_coefficient(
    prop: Any, op: Callable[[Any, Any], Any], value: Any, coeff: float, df_value: float
) -> float:
    if op(prop, value):
        return df_value * coeff
    else:
        return df_value

def group_dataframe(df: pd.DataFrame, grouping_cfg: list) -> pd.DataFrame:
    for grouping in grouping_cfg:
        df[grouping["name"]] = grouping["ungrouped_name"]

        for group in grouping["groups"]:
            if group["lower_limit"] is None and group["upper_limit"] is None:
                raise KeyError(
                    f"Every group must have either an upper limit, a lower limit, or both! "
                    f"This is not the case for group_name: {group['group_name']}."
                )
            elif group["lower_limit"] is None:
                df[grouping["name"]] = [
                    group["group_name"]
                    if p <= group["upper_limit"]
                    else df[grouping["name"]][i]
                    for i, p in enumerate(df[grouping["property"]])
                ]
            elif group["upper_limit"] is None:
                df[grouping["name"]] = [
                    group["group_name"]
                    if group["lower_limit"] <= p
                    else df[grouping["name"]][i]
                    for i, p in enumerate(df[grouping["property"]])
                ]
            else:
                df[grouping["name"]] = [
                    group["group_name"]
                    if group["lower_limit"] <= p <= group["upper_limit"]
                    else df[grouping["name"]][i]
                    for i, p in enumerate(df[grouping["property"]])
                ]

        if isinstance(grouping["collect_data"], list):
            coll_df = pd.DataFrame()
            coll_df[grouping["name"]] = df[grouping["name"]]
            for coll in grouping["collect_data"]:
                coll_df[coll["collect_field"]] = df[coll["collect_field"]]
            coll_df.drop_duplicates(inplace=True)
            coll_df.to_csv(grouping["collect_file"])

    return df

@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(cfg: DictConfig):
    data_dir = cfg.data
    agg = cfg.aggregation
    aggregation_function: Any = OmegaConf.to_object(agg.agg_functions)

    files_list = [
        {
            "name": "ecs",
            "path": data_dir.ecs,
            "files": os.listdir(data_dir.ecs),
            "fields": agg.ecs_fields,
            "abs_col": "Abundance_RPKs",
            "out_path": data_dir.ecs,
        },
        {
            "name": "tax",
            "path": data_dir.tax,
            "files": os.listdir(data_dir.tax),
            "fields": agg.tax_fields,
            "abs_col": "Metaphlan2_Analysis",
            "out_path": data_dir.tax,
        },
        {
            "name": "pwy",
            "path": data_dir.pwy,
            "files": os.listdir(data_dir.pwy),
            "fields": agg.pwy_fields,
            "abs_col": "Abundance",
            "out_path": data_dir.pwy,
        },
    ]
    df_meta = pd.read_excel(data_dir.metadata, index_col=False, header=0)
    df_meta.drop(
        ["urls", "Type"],
        axis=1,
        inplace=True,
    )
    df_meta.drop_duplicates(inplace=True)
    df_meta = df_meta.reset_index(drop=True)

    for item in files_list:
        aggregate_on: Any = OmegaConf.to_object(agg.agg_on_fields)

        aggregate_on.extend(item["fields"])

        df = aggregate(
            item["path"],
            item["files"],
            df_meta,
            item["abs_col"],
            aggregate_on,
            aggregation_function,
            OmegaConf.to_object(agg.data_modification),
        )
        # create folder if it does not exist
        if not os.path.exists(item["out_path"]):
            os.makedirs(item["out_path"])
        df.to_csv(f"{item['out_path']}/agg_{item['name']}.csv")

    print("Done.")


if __name__ == "__main__":
    main()
