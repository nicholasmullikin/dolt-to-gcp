from doltpy.cli import Dolt

from doltpy.cli.read import read_pandas
import sqlalchemy
import pandas as pd
import sys
from sqlalchemy import create_engine
from tqdm import tqdm




engine = create_engine("mysql://user:password@ip:3306/data?charset=utf8")

repo = Dolt('hospital-price-transparency')

query = "SELECT code, short_description, long_description from cpt_hcpcs WHERE (short_description like '%%blood%%' and " \
        "short_description like '%%transfusion%%')         or (long_description like '%%blood%%' and             " \
        "long_description like '%%transfusion%%'); "
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def insert_with_progress(df, table_name):
    chunksize = int(len(df) / 10)
    with tqdm(total=len(df)) as pbar:
        for i in range(10):
            pos = chunksize * i
            cdf = df.iloc[pos:pos+chunksize,:]
            cdf.to_sql(name=table_name, con=engine, if_exists="append", index=False)
            pbar.update(chunksize)
            tqdm._instances.clear()


cpt_hcpcs_df = read_pandas(repo, "cpt_hcpcs")
insert_with_progress(cpt_hcpcs_df, "cpt_hcpcs")

hospitals_df = read_pandas(repo, "hospitals")
insert_with_progress(hospitals_df, "hospitals")

prices_df = read_pandas(repo, "prices")
insert_with_progress(prices_df, "prices")
