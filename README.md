### Execution process
1. run `docker-compose up` to build and run the project
2. run `docker ps` to check if the container is running
3. run `docker exec -it [CONTAINER_ID (geo2ip_web)] bash` to get into container
4. run `python manage.py create_tables --date [datetime ex.20210921]` to create tables
5. run `python manage.py transform_upload --date [datetime ex.20210921]` to transform and upload data to postgre DB
6. run `python manage.py analyze --date [datetime ex.20210921]` to get top 10 countries with most unique ip range 
