import argparse
import re
import sys
import numpy as np
import pandas as pd 

def find_header(path: str, search_range: int = 20) -> int:
  temp = pd.read_excel(path, header=None)
  search_labels = {"Type","Date", "Name", "Amount", "Memo", "Description", "Customer/Grant: Company Name"}
  for i in range(min(search_range, len(temp))):
    row_vals = set(str(x).strip() for x in temp.iloc[i].tolist())
    if search_labels.issubset(row_vals):
      return i
    return 5
