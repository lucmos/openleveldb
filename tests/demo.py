from time import sleep

import numpy as np
import plyvel
import streamlit as st
from openleveldb.database import LevelDB
from tqdm import tqdm

db = LevelDB("/home/luca/Scrivania/azz", allow_multiprocessing=True)

for x in range(10):
    db[f"key{x}"] = f"value{x}"

st.checkbox("make it crash")

num = 100
p = st.progress(0)
for x in range(num + 1):
    int(x / num * 100)

    p.progress(int(x / num) * 100)
    db[f"key{x % 10}"]
    sleep(0.1)
