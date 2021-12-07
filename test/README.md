# Testing the target REST API

There are many different ways how to test targets automatically. Inspiration can be found for example [here](https://github.com/datamill-co/target-postgres/tree/master/tests) or [here](https://github.com/transferwise/pipelinewise-target-snowflake/tree/master/tests). It is one of the planned features for this repo to include automated unit tests. For now, however, using this readme and code in this folder you can test `target-rest` manually.

## Environment setup

* Use Conda environment manager to create new environment: `conda env create -f environment.yml`
* Activate the environment: `conda activate target-rest-test`

## Running the target

The environment created in the previous step contains `tap-csv` that will be used in this test to feed data to `target-rest`. Another important part of the test setup is the `rest_server.py` with a very simple flask server with one endpoint `test` expecting `POST` request with JSON data.

1) Start the REST server with: `python rest_server.py`
2) Let the server running and in different terminal window change directory to root folder of this project (`cd ..`)
3) Expecting default conda installation in folder `anaconda3` in your `home` folder you can run the test with:

`python ~/anaconda3/envs/target-rest-test/bin/tap-csv -c test/config_tap_csv.json | python target_rest/__init__.py -c test/config_target_rest_api.json`

4) REST server should print data from `test/test.csv` line by line like this:

```bash
{
    "a": "1",
    "b": "2",
    "c": "3"
}
127.0.0.1 - - [07/Dec/2021 11:08:44] "POST /test HTTP/1.1" 200 -
{
    "a": "1",
    "b": "2",
    "c": "3"
}
127.0.0.1 - - [07/Dec/2021 11:08:44] "POST /test HTTP/1.1" 200 -
{
    "a": "5",
    "b": "6",
    "c": "9"
}
127.0.0.1 - - [07/Dec/2021 11:08:44] "POST /test HTTP/1.1" 200 -
```
