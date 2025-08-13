import argparse, yaml, pandas as pd
from utils import logger, db_connect
from transform import clean_chunk
from load import create_table_if_not_exists, upsert_dataframe

def run(cfg):
    src = cfg["source"]; tgt = cfg["target"]; opt = cfg["options"]
    req = opt["fail_on_missing"]; dcols = opt.get("date_columns", [])
    pk = tgt.get("pk", ["order_id"])[0]
    path = src["path"]; chunk = src.get("chunk_size", 5000)
    table = tgt["table"]; conn_str = tgt["conn_str"]

    rows_in, rows_out = 0, 0
    with db_connect(conn_str) as conn:
        create_table_if_not_exists(conn, table)
        for df in pd.read_csv(path, chunksize=chunk):
            rows_in += len(df)
            df = clean_chunk(df, req_cols=req, date_cols=dcols)
            if not df.empty:
                upsert_dataframe(conn, df, table, pk=pk)
                rows_out += len(df)

    logger.info(f"Done! in={rows_in}, loaded={rows_out}, table={table}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    run(cfg)
