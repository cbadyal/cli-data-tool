import argparse
import re
import sys
import numpy as np
import pandas as pd 
from typing import List, Optional

#global vars
STOPWORDS = {
    "inc","llc","l.l.c","co","company","corp","corporation","foundation","trust","trustees",
    "the","and","of","for","a","an","center","centre","committee","fund","project","group","partners", "mobilecause", "benevity" #either we don't need addresses for these companies or filtering out common words among all separate companies
}#set lookup is O(1)
TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
DEST_ADDR_COLS = ["Billing Street","Billing City","Billing State/Province","Billing Zip/Postal Code","Billing Country"]

def find_header(path: str, search_range: int = 20) -> int:
  temp = pd.read_excel(path, header=None)
  search_labels = {"Type","Date", "Name", "Amount", "Memo", "Description", "Customer/Grant: Company Name"}
  for i in range(min(search_range, len(temp))):
    row_vals = set(str(x).strip() for x in temp.iloc[i].tolist())
    if search_labels.issubset(row_vals):
      return i
    return 5

def choose_addr_cols(df: pd.DataFrame) -> List[str]:
  groups = [
        ["Billing Street", "Billing City", "Billing State/Province", "Billing Zip/Postal Code", "Billing Country"],
        ["Address", "City", "State", "Zip", "Country"],
        ["Street", "City", "State", "PostalCode", "Country"]
        ]
  for g in groups:#for each list of addr labels
    if all(c in df.columns for c in g):
      return g
  addr_pattern = re.compile(r"(address|addr|street|zip|postal)", re.IGNORECASE)
  return [c for c in df.columns if addr_pattern.search(str(c))]#building src addr cols based on what info we can find in the file 

def tokens(name: Optional[str]) -> set:
  if pd.isna(name):
        return set() #if name is empty return empty set 
  s = str(name).lower()
  toks = TOKEN_RE.findall(s) #['alice', 'alice', 'alice']
  toks = [t for t in toks if t not in STOPWORDS and len(t) > 1]#could just adjust the regex, but this is easier to read/change
  return set(toks) #{'alice'}

def jaccard(a: set, b: set) -> float:
  if not a or not b:
      return 0.0
  inter = len(a & b)
  union = len(a | b)
  return (inter / union) if union else 0.0

def main():
  p = argparse.ArgumentParser()
  p.add_argument("--netsuite", required=True, help="Path to Netsuite Excel export")
  p.add_argument("--addresses", required=True, help="Path to master addresses Excel file")
  p.add_argument("--out", required=True, help="Output Excel path")
  p.add_argument("--start", type=int, default=None, help="Start row index in cleaned dataframe")
  p.add_argument("--end", type=int, default=None, help="End row index (exclusive)")
  p.add_argument("--threshold", type=float, default=0.40, help="Minimum name overlap (0-1) required to copy address")
  args = p.parse_args()

  #load Netsuite and addresses files, df1=netsuite df2=addresses
    
  hdr = find_header(args.netsuite)
  df1 = pd.read_excel(args.netsuite, header=hdr)
  df2 = pd.read_excel(args.addresses).dropna(how="all")

  #id columns
  #col1_name is "customer/grant: company name" from netsuite file
  #col2_name is "account name" in addresses file

  col1_name = "Customer/Grant: Company Name" if "Customer/Grant: Company Name" in df1.columns else ("Name" if "Name" in df1.columns else None)
  if not col1_name:
      sys.exit("Couldn't find 'Customer/Grant: Company Name' or 'Name' column in Netsuite file")
  col2_name = "Account Name" if "Account Name" in df2.columns else ("Name" if "Name" in df2.columns else None)
  if not col2_name:
      sys.exit("Couldn't find 'Account Name' or 'Name' column in addresses file")
    
  #check for/create destination address cols in df1
  for c in DEST_ADDR_COLS:
      if c not in df1.columns:
        #explicitly building the col to avoid FutureWarning
        df1[c] = pd.Series(pd.NA, index=df1.index, dtype="string")
      else:
          df1[c] = df1[c].astype("string")
    
  #source addr cols in df2
  src_addr_cols = choose_addr_cols(df2)
  if not src_addr_cols:
    sys.exit("Couldn't detect address columns in address file")
    
  #compute tokens for df2 and add tokens column to dataframe
  df2["_name_tokens"] = df2[col2_name].apply(tokens)

  #select range
  start = args.start if args.start is not None else 0
  end = args.end if args.end is not None else len(df1)

  #extra cols for more info
  additional_cols = ["Matched Account Name", "Name Match Score", "Needs Review", "Review Note"]
  for c in additional_cols:
    if c not in df1.columns:
      df1[c] = pd.Series(pd.NA, index=df1.index, dtype="string")
    

    




    



