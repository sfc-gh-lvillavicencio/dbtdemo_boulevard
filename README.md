----------------------------------------------------
Working With python Enviroments:
----------------------------------------------------

https://docs.getdbt.com/docs/local/install-dbt?install-method=pip


Python virtual environment is created in venv folder (in this repo)

1.create the environment with (already done):
python3 -m venv venv

2.activate with :
source venv/bin/activate

3.install dbt and libraries:
python -m pip install --pre dbt
dbt --version
pip install dbt-snowflake

4.initializae dbt:
dbt init

5.change configuration file:
dbt run --profiles-dir /Users/lvillavicencio/Documents/Github/dbtdemo_boulevard

6. test dbt conection:
dbt debug



Welcome to your new dbt project!

Core:
  - installed: 1.12.4
  - latest:    1.12.4 - Up to date!

Plugins:
  - snowflake: 1.12.0 - Up to date!

### Using the starter project

Try running the following commands:
- dbt run --select example
- dbt run --select blvd_ai.ml_demand_model
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
