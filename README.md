# HYFAA platform backend

Flask backend for the HYFAA web platform. 

Provides an API on top of the [PostGIS database](https://github.com/OMP-IRD/hyfaa-database/).

## Installation

**Installation via `requirements.txt`**:

```shell
git clone https://github.com/OMP-IRD/hyfaa-backend.git
cd hyfaa-backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# configure the DB using environment variable: adjust if necessary
export DATABASE_URI=postgresql://hyfaa_backend:hyfaa_backend@localhost:5432/mgb_hyfaa
cd src && flask run
```

## Using docker

```shell
make docker-build
docker run --rm -e DATABASE_URI=postgresql://hyfaa_backend:hyfaa_backend@[DB_HOST]:5432/mgb_hyfaa \
           -p 5000:5000 pigeosolutions/hyfaa-backend:1.0
```
Some comments:
  * When giving the DB connection string, you will have to provide a DB host that is visible by the container. 
  _`localhost` is obviously not valid, since it refers to the container's local host._

  * it is of course better used i relation with the other related services, so we advise to use the docker-composition
   given in the parent project,  https://github.com/OMP-IRD/hyfaa-mgb-platform
   
 To run the netcdf publication script, you might run
 ```shell
make docker-build
docker run  -e DATABASE_URI_RW=postgresql://hyfaa_publisher:hyfaa_publisher@[DB_HOST]:5432/mgb_hyfaa \
            -v [path-to-scheduler-work-folder]:/hyfaa-scheduler/data \
            -p 5000:5000 pigeosolutions/hyfaa-backend:1.0 \
            python3 /hyfaa-backend/app/scripts/hyfaa_netcdf2DB.py /hyfaa-scheduler/data/
```
using for instance 
`~/hyfaa-mgb-platform/hyfaa-scheduler/work_configurations/operational_niger_gsmap/` 
for path-to-scheduler-work-folder

Again, it's better to be run using docker-compose. 

At some point, this script will probably be moved under the scheduler app, where it might be more efficient to be.