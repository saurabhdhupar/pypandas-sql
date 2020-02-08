# pypandas-sql

[![CircleCI](https://circleci.com/gh/saurabhdhupar/pypandas-sql.svg?style=svg)](https://circleci.com/gh/saurabhdhupar/pypandas-sql)
[![codecov](https://codecov.io/gh/saurabhdhupar/pypandas-sql/branch/dev/graphs/badge.svg)](https://codecov.io/gh/saurabhdhupar/pypandas-sql)

## pypandas-sql

This package is designed to make it easier to get data from most commonly used databases/data warehouse into a pandas DataFrame using SQL.
Allow users to manage credentials by doing a one time configuration thus avoiding hardcoding them in code. 
Initial release supports Redshift with plans to support other ones. [see issues](https://github.com/saurabhdhupar/pypandas-sql/issues)
Package only supports python3.6.

## Installation

```python
pip install pypandas-sql
```

## Support Commands

```python
pypandasql -help
Usage: pypandasql [OPTIONS] COMMAND [ARGS]...

Options:
  -help  Show this message and exit.

Commands:
  redshift
```

## Configure Redshift Credentials 
#### Enter Details (Host Name , Port, User Name, Password)
```python
pypandasql redshift configure
Redshift Host Name: xxx
Redshift Port: 123
Redshift User Name: xxx
Redshift Password: xxx
```

## Usage

### Redshift Example (SQL -> Pandas)

#### Initialize 
```python
from pypandas_sql.queryengine.redshift_query_engine import RedshiftQueryEngine
query_engine = RedshiftQueryEngine()
```

#### Query Redshift Table and get a Pandas DataFrame
```python
df = query_engine.get_pandas_df(sql='<select * from part limit 10>', schema='<dev>')

>>> df

   p_partkey                p_name  p_mfgr p_category   p_brand1    p_color                   p_type  p_size p_container
0         13            blue olive  MFGR#5    MFGR#53  MFGR#5333      ghost  MEDIUM BURNISHED NICKEL       1  JUMBO PACK
1         21  aquamarine firebrick  MFGR#2    MFGR#25  MFGR#2517      lemon      SMALL BURNISHED TIN      31     MED BAG
2         30           beige steel  MFGR#5    MFGR#51  MFGR#5115      blush       PROMO ANODIZED TIN      17      LG BOX
3         41         ghost antique  MFGR#5    MFGR#51  MFGR#5120  burlywood     ECONOMY ANODIZED TIN       7    WRAP JAR
4         45        medium frosted  MFGR#5    MFGR#55  MFGR#5522      lemon     SMALL BRUSHED NICKEL       9    WRAP BAG
5         80          saddle brown  MFGR#4    MFGR#45  MFGR#4527     tomato       PROMO PLATED BRASS      28     MED CAN
6         88           olive azure  MFGR#1    MFGR#12  MFGR#1230       blue      PROMO PLATED COPPER      16     SM CASE
7        100          orange khaki  MFGR#3    MFGR#35  MFGR#3522      light     ECONOMY ANODIZED TIN       4      LG BAG
8        103            cream lime  MFGR#5    MFGR#52  MFGR#5236       navy      MEDIUM PLATED BRASS      45   WRAP DRUM
9        106            beige deep  MFGR#3    MFGR#35   MFGR#357   cornsilk      MEDIUM PLATED BRASS      28   WRAP DRUM

```

#### Query Redshift Table and get a Pandas DataFrame (using templated parameters)
```python
s = 'select count(*) as ttl_customer, c_city, c_region from customer where c_mktsegment = %(segment)s group by c_region,c_city;'
df = query_engine.get_pandas_df(sql=s, schema='dev', parameters={"segment":"FURNITURE"})

>>> df
     ttl_customer      c_city      c_region
0              72  INDIA    7  ASIA        
1              88  JAPAN    7  ASIA        
2              88  CHINA    8  ASIA        
3              76  CANADA   7  AMERICA     
4             102  SAUDI ARA6  MIDDLE EAST 
..            ...         ...           ...
245            70  FRANCE   6  EUROPE      
246            78  IRAQ     7  MIDDLE EAST 
247            80  IRAQ     1  MIDDLE EAST 
248            84  UNITED ST3  AMERICA     
249            68  ETHIOPIA 1  AFRICA      

[250 rows x 3 columns]
```
