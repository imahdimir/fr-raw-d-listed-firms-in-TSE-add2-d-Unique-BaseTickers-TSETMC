##

"""

    """

##

from pathlib import Path

import pandas as pd

from githubdata import GithubData
from mirutil import funcs as mf


repo_url = 'https://github.com/imahdimir/raw-d-listed-firms-in-TSE'
tic2btic_repo_url = 'https://github.com/imahdimir/d-Ticker-2-BaseTicker-map'
btics_repo_url = 'https://github.com/imahdimir/d-Unique-BaseTickers-TSETMC'

btick = 'BaseTicker'
namad = 'نماد'
naam = 'نام - ا'
tick = 'Ticker'
cname = 'CompanyName'

def main() :

  pass

  ##
  repo = GithubData(repo_url)
  repo.clone_overwrite_last_version()
  ##
  dfpn = repo.local_path / 'بورس اوراق بهادار تهران - لیست شرکت ها.xlsx'
  df = pd.read_excel(dfpn)
  ##
  df[namad] = df[namad].apply(mf.norm_fa_str)
  ##
  msk = df[namad].ne(namad)
  df = df[msk]

  ##
  ptr = 'ح' + '\s?\.\s.+'
  msk = df[naam].str.fullmatch(ptr)
  df = df[~ msk]
  ##
  ptr = 'ح' + '\..+'
  msk = df[naam].str.fullmatch(ptr)
  df = df[~ msk]
  ##
  ptr = 'ح' + '\s.+'
  msk = df[naam].str.fullmatch(ptr)
  df = df[~ msk]
  ##

  tic2btic_repo = GithubData(tic2btic_repo_url)
  tic2btic_repo.clone_overwrite_last_version()
  ##
  mdfpn = tic2btic_repo.data_fps[0]
  mdf = pd.read_parquet(mdfpn)
  ##
  mdf = mdf.set_index(tick)
  ##
  df[btick] = df[namad].map(mdf[btick])

  ##
  msk = df[namad].eq('سابیک1')
  df.at[msk, namad] = 'سآبیک1'
  ##
  msk = df[btick].isna()
  df1 = df[msk]
  ##
  for _, row in df1.iterrows():
    print('"'+row[namad]+'":None,')
  ##
  nmds = {
      "آسیاتک1"  : None ,
      "ثملی1"    : None ,
      "اروند1"   : None ,
      "پرشیا1"   : None ,
      "برانسفو1" : None ,
      "سآبیک1"   : None ,
      "ومعین1"   : None ,
      "وسمحال1"  : None ,
      "شجی1"     : None ,
      "ولنوین1"  : None ,
      "سینا1"    : None ,
      "اجداد1"   : None ,
      }

  msk = df[btick].isna()
  msk &= df[namad].isin(nmds.keys())

  df.loc[msk, btick] = df[namad].str[:-1]
  ##
  msk = df[btick].isna()
  df1 = df[msk]
  ##
  df[cname] = df[naam]
  ##
  df2 = df[[btick, cname]]
  ##

  btics_repo = GithubData(btics_repo_url)
  btics_repo.clone_overwrite_last_version()
  ##
  bdfpn = btics_repo.data_fps[0]
  bdf = pd.read_parquet(bdfpn)
  ##
  bdf = bdf.reset_index()
  ##
  bdf = bdf.merge(df2, how = 'outer')

  ##
  bdf = bdf.set_index(btick)
  ##
  bdf.to_parquet(bdfpn)
  ##
  commit_msg = 'added new BaseTickers & added CompanyName col'
  btics_repo.commit_and_push_to_github_data_target(commit_msg)

  ##
  btics_repo.rmdir()
  repo.rmdir()
  tic2btic_repo.rmdir()

  ##




  ##

  ##





  ##




  ##


##