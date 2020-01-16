import time

import streamlit as st
from openleveldb.database import LevelDB


@st.cache(hash_funcs={LevelDB: id}, allow_output_mutation=False)
def get_db() -> LevelDB:
    return LevelDB("/tmp/testdb/")


db = get_db()

db["key"] = "value"

st.checkbox("make it not crash :)")

num = 20

p = st.progress(0)

for x in range(num + 1):
    time.sleep(0.1)
    a = ["key"]
    p.progress(int(x / num * 100))
