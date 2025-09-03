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

  ns = df1.iloc[start:end].copy()#ensure deep copy is made for netsuite file range
  ns["_rid"] = ns.index #for when we explode tokens later

  #tokenize names
  ns["_name_tokens"] = ns[col1_name].apply(tokens)
  df2["_aid"] = df2.index
  df2["_name_tokens"] = df2[col2_name].apply(tokens)

  #calculate token counts for union calc (ns = netsuite, ad = addresses)
  ns_sizes = ns[["_rid", "_name_tokens"]].assign(ns_sz=lambda d: d["_name_tokens"].apply(len))
  ad_sizes   = df2[["_aid","_name_tokens"]].assign(ad_sz=lambda d: d["_name_tokens"].apply(len))

  #exploding tokens + simpler col name for merging on
  ns_tok = (ns[["_rid","_name_tokens"]].explode("_name_tokens").dropna()
                .rename(columns={"_name_tokens": "tok"}))
  
  ad_tok   = (df2[["_aid","_name_tokens"]].explode("_name_tokens").dropna()
                .rename(columns={"_name_tokens": "tok"}))
  
  #candidates for a shared name (based on if they share tok(s))
  cand = ns_tok.merge(ad_tok, on="tok", how="inner")[["_rid","_aid","tok"]]

  #intersection for jaccard 
  inter = (cand.drop_duplicates(["_rid","_aid","tok"])
                  .groupby(["_rid","_aid"])["tok"].count()
                  .reset_index(name="inter"))#reset index to "convert back" to df 
  
  #calculating jaccard
  pairs = (inter.merge(ns_sizes[["_rid","ns_sz"]], on="_rid", how="left")
             .merge(ad_sizes[["_aid","ad_sz"]], on="_aid", how="left"))
  pairs["union"] = (pairs["ns_sz"] + pairs["ad_sz"] - pairs["inter"]).clip(lower=1)
  pairs["score"] = pairs["inter"] / pairs["union"]

  #only keeping _rid/_aid combinations with best score 
  best_idx = pairs.groupby("_rid")["score"].idxmax()
  best = pairs.loc[best_idx]
  best = best[best["score"]>=args.threshold]

  #merging to match addresses 
  take_cols = [col2_name] + src_addr_cols
  best = best.merge(df2[["_aid"] + take_cols], on="_aid", how="left")

  #aligning rows to ns file and merging relevant best columns
  out = ns[["_rid"]].merge(best[["_rid","_aid","score"] + take_cols],#dont rllly need aid since best already includes addresses
        on="_rid", how="left"
  )

  #review columns
  ns["Matched Account Name"] = out[col2_name].astype("string")
  ns["Name Match Score"] = (out["score"].fillna(0)*100).round(1).astype("Float64")
  ns["Needs Review"] = out["score"].isna().astype("boolean")
  ns["Review Note"] = ns["Needs Review"].map({True: "Low overlap", False: "Matched"}).astype("string")

  #copying addresses for matched rows if columns match
  for dest in DEST_ADDR_COLS:
        if dest in src_addr_cols and dest in out.columns:#TODO: src/dest addr col mappings
            ns[dest] = out[dest].astype("string")

  #denoting empty name 
  empty_name = ns[col1_name].isna() | (ns[col1_name].astype(str).str.strip() == "")
  ns.loc[empty_name, DEST_ADDR_COLS] = pd.NA
  ns.loc[empty_name, "Matched Account Name"] = pd.NA
  ns.loc[empty_name, "Name Match Score"] = 0.0
  ns.loc[empty_name, "Needs Review"] = False
  ns.loc[empty_name, "Review Note"] = "Missing name"

  #putting processed slice back into df1 
  df1.loc[ns.index, ns.columns] = ns
  df1.to_excel(args.out, index=False)
  print(f"Saved: {args.out}")
  print(f"Processed rows [{start}:{end}) with threshold={args.threshold:.2f}")

  if __name__ == "__main__":
     main()




  






  





  


    




    



