# e-soh-api
The API for retrieving data from e-soh datastore

## Dependencies
You should install this package locally before starting the server.
You will need a pygeoapi install that is later then 0.15, as 0.15 only support pydantic v1.x.x.
Make sure pydantic=~2.3 is installed.

## Starting
Before starting pygeoapi, you need to sourve the env.var file from the top level of this repository. Then start pygeoapi with `pygeoapi serve --flask`. The api should start on port 8000. You might need to change the gRPC IP `dshost` in `pygeoapi-config.yml` before starting the server.


