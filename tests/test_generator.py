from pathlib import Path
from pipeline.generate import generate

def test_generator_is_reproducible(tmp_path):
    a,b=tmp_path/"a.csv",tmp_path/"a_customers.csv"
    c,d=tmp_path/"b.csv",tmp_path/"b_customers.csv"
    generate(a,b,25,seed=9); generate(c,d,25,seed=9)
    assert a.read_bytes() == c.read_bytes()
    assert b.read_bytes() == d.read_bytes()
