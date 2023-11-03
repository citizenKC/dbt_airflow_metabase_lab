import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time
import os
from dotenv import load_dotenv
load_dotenv()
import logging

USER = os.environ.get('PG_USER')
PASS = os.environ.get('PG_PASS')
DB = os.environ.get('PG_DB')
PRT = os.environ.get("PG_PORT_2")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_data(parquet_file, table_name):
    parquet_file = pq.ParquetFile(parquet_file)
    # trips = parquet_file.read().to_pandas()

    engine = create_engine(f"postgresql://{USER}:{PASS}@de_postgres:{PRT}/{DB}")

    # trips.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace", index=False)

    for batch in parquet_file.iter_batches(batch_size=100000):
        t_start = time()
        batch_df = batch.to_pandas()
        batch_df.columns = [c.lower() for c in batch_df.columns]
        batch_df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
        t_end = time()
        logger.info("inserted next chunk in %.3f seconds" % (t_end - t_start))
        #print("inserted next chunk in %.3f seconds" % (t_end - t_start))
