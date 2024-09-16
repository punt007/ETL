# Project ETL: 
Extract | Transform | Load

## Introduction

No matter how well the data warehouse team organize their warehouse, Data will never be 100% ready to use for us data nerd. I'd like to write about one specific case that I came across that I'd like to share here. One case, I need to create performance report of AI voice bot as saleperson. The performance data is organized by warehouse, but due resourse optimization, they keep data in JSON alike data. 

### Datasets
Instead of hundreds of columns, they store everything in single column, in "dictionary" alike in a single column. For example, 


|Feature|Type|Dataset|
|---|---|---|
|sample|samplex|"Key1" : "Value1", "Key2"  : "Value2", "Key3": Value3"......|
|sample2|sampley|"Key1" : "ValueA", "Key2"  : "ValueB", "Key3": ValueC"......|


There is no way that these dataset can be used for anthing





## The process



### Extract

    Warehouse is in Oracle environment, so I use simply SQL to extract date


    Select /* PARALLEL(16) */ 
    FROM
          table_name
    WHERE
        date_time between to_date('2020-01-01', 'YYYY-MM-DD') AND ......

---

### Transform

    Data needed to be transform into seperate column according to "key".
    This part I'm using "PYTHON" to create funtion to extract data and place them into seperate columns


    # Code to create function to parse data

    def parse_data(input):
        key_value_pairs = input.split(',')
        i = {}
        for pair in key_value_pairs:
            parts = pair.split(':')
            if len(parts) == 2:
                key = parts[0].strip()
                value = parts[1].strip()
                data[key] = value
            else:
                pass
        return i


    Use the above function to create dataframe as desired.


---
### Load
    Once I got dataframe that contains data that I need, it is time to "LOAD" this back to warehouse.
    This part I also use "PYTHON" with sqlalchemy library.

    I create function to load dataframe into database

    # Code to create function to write data (table) into Oracle database

    from sqlalchemy import types,text
    from pandas.api.types import is_string_dtype,is_categorical_dtype,is_datetime64_any_dtype

    def change_type_and_upload(df,table_name,engine = tdm_engine,schema='xxyyzz',if_exists='replace',dtype_dict = None,chunksize = 5000):
        if  dtype_dict == None:
            dtype_dict = {c:types.VARCHAR(200) for c in  df.columns if is_categorical_dtype(df[c]) or is_string_dtype(df[c])}
            for c in df.columns :
                if is_datetime64_any_dtype(df[c]):
                    dtype_dict[c] = sqlalchemy.DateTime
    df.to_sql(name=table_name,con=engine,schema=schema,index=False, if_exists=if_exists, dtype=dtype_dict,chunksize = 5000)



    Data will be written as "table_name" in database
