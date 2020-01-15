import numpy as np
import streamlit as st
from openleveldb.client import LevelDBClient
from tqdm import tqdm

db = LevelDBClient.get_instance()

st.title("Test")
nu = 500

writing = st.progress(0)
for x in tqdm(range(nu), desc="writing"):
    key = f"{x}"
    value = np.random.rand(3, 1000)
    db[key] = value
    writing.progress(int(x / nu * 100) + 1)
writing.progress(100)

reading = st.progress(0)
for x in tqdm(range(nu), desc="reading"):
    key = f"{x}"
    value = db[key]
    reading.progress(int(x / nu * 100) + 1)
reading.progress(100)

# # fmt: off
# # fmt: on
#
# from collections import namedtuple
# from pathlib import Path
#
# import numpy as np
# from openleveldb.dbconnector import LevelDB
#
# dataset_root = "test.db"
# db = LevelDB.get_instance(dataset_root)
#
# #
# # samples = []
# # for i in range(10000):
# #     samples.append(
# #         mesh(
# #             vert=np.random.rand(100, 3),
# #             triv=np.random.rand(100, 3),
# #             evals=np.random.rand(500),
# #         )
# #     )
#
# # write to db
# # for i, sample in tqdm(enumerate(samples)):
# #     key = f"{i}"
# #     db["triv", key] = sample.triv
# #     db["vert", key] = sample.vert
# #     db["evals", key] = sample.evals
#
#
# # read from db
# key = f"{0}"
# triv = db["triv", key]
# vert = db["vert", key]
# evals = db["evals", key]
#
# vert
#
# # print(triv)
# # print(vert)
# # print(evals)
# # assert False
# # for x in db.db.iterator(include_value=False):
# #     print(x)
# #     pass
# # print(serializer.decode(x))
# print(dblen(db["triv", ...]))
# # for x in db:
# #     print(x)
# #     assert False
