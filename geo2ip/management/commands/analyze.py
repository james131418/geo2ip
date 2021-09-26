import logging
import os

import psycopg2
import pandas.io.sql as psql
from django.core.management.base import BaseCommand, CommandError


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Data analysis"

    TOP_10_IP_RANGE_COUNTRY = "/geo2ip/temp/top_10_ip_range_country.csv"

    def add_arguments(self, parser):
        parser.add_argument(
            "--date",
            action="store",
            dest="date",
            default=20210921,
            help="specify date for data",
        )

    def handle(self, *args, **options):
        if not options["date"]:
            raise CommandError("Option `--date=...` must be specified.")

        conn = None
        try:
            # connect to the PostgreSQL server
            conn = psycopg2.connect(
                database="postgres",
                user="postgres",
                password="postgres",
                host="db",
                port="5432",
            )
            # data operation
            df_ip = psql.read_sql(
                "SELECT network, geoname_id FROM geoip2_network_{date}".format(
                    date=options["date"]
                ),
                conn,
            )
            df_country = psql.read_sql(
                "SELECT country_name, geoname_id FROM country_geoname_id_{date}".format(
                    date=options["date"]
                ),
                conn,
            )
            df_ip = df_ip.drop_duplicates(subset=["network"])
            df_ip_country = df_ip.merge(df_country, on="geoname_id", how="left")
            df_count = (
                df_ip_country.groupby("country_name")
                .count()
                .sort_values(by="network", ascending=False)
                .reset_index()
            )
            # save top 10 countries to temp dir
            df_count["country_name"].head(10).to_csv(
                self.TOP_10_IP_RANGE_COUNTRY, index=False, header=False, sep="\t"
            )

            cur = conn.cursor()
            # insert data to tables
            with open(self.TOP_10_IP_RANGE_COUNTRY, "r") as f:
                cur.copy_from(
                    f,
                    "top_10_ip_range_country_{date}".format(date=options["date"]),
                    sep="\t",
                )

            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
            print("data analysis successfully........")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            if conn is not None:
                conn.close()
