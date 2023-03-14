# dbt_smart_run
# 📣 What does this dbt package do?
Do you find yourself wasting time looking through your data lineage DAG trying to figure out which models to run?  What about wanting to save on the costs of running your dbt models? Do your dbt runs take forever?

To solve these problems, we developed “dbt smart run”.  Our way of running only the models that need to run without wasting your brain-power to figure out how to craft your dbt run command. 

Note: This package is currently only supported on BigQuery.  However, if you use another data warehouse, you can use majority of the code and make the necessary edits to work with your data warehouse.

# 🎯 How do I use the dbt package?
## Step 1: Copy all required file from this repo to your own.

+ dbt_smart_run.py

+ All files in the macros folder, including:
  + dbt_smart_run.sql
  + generate_schema_name.sql
  + get_all_upstream_reference_models.sql
  + is_valid_model_list.sql
  + reset_dev_environment.sql
  + reset_dev_for_list_of_models.sql

> Note: These files will need to be stored in your dbt project's macro folder.

+ packages.yml
  + If you don't already have a dependency on dbt_utils, you will need this package to use dbt_smart_run.

+ requirements.txt

## Step 2: Install the dbt_utils package
(Skip if you already have a dependency on dbt_utils)
Be sure you've included the dbt_utils package to your `packages.yml` file
> TIP: Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions or [read the dbt docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=0.8.0", "<0.9.0"]
```

## Step 3: Install required python packages
Install dependencies by running the following:
 ```
 $ pip3 install -r ../requirements.txt
 ```

## Step 4: Run dbt_smart_run
From your terminal, run the following command to run your dbt models. 

For one target model:
```
$ python3 dbt_smart_run.py -targets your_target_model
```

For multiple target models:
```
$ python3 dbt_smart_run.py -targets your_first_target_model your_second_target_model
```

Here's an example of what to expect from your terminal output:
