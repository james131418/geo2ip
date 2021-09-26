import logging
import os

import psycopg2
import pandas as pd
import numpy as np
from django.core.management.base import BaseCommand, CommandError

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Transform decimal IP to string and upload extracted data"

    EXTRACTED_DATA_DIR = "/geo2ip/GeoLite2-Country-CSV_{date}"
    GEOIP2_NETWORK_OUTPUT_PATH = "/geo2ip/temp/geo2ip_network.csv"
    COUNTRY_GEONAME_ID_OUTPUT_PATH = "/geo2ip/temp/country_geoname_id.csv"

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

        df_ipv4 = pd.read_csv(
            os.path.join(
                self.EXTRACTED_DATA_DIR.format(date=options["date"]),
                "GeoLite2-Country-Blocks-IPv4.csv",
            )
        )
        df_ipv6 = pd.read_csv(
            os.path.join(
                self.EXTRACTED_DATA_DIR.format(date=options["date"]),
                "GeoLite2-Country-Blocks-IPv6.csv",
            )
        )
        df_country_geoname_id = pd.read_csv(
            os.path.join(
                self.EXTRACTED_DATA_DIR.format(date=options["date"]),
                "GeoLite2-Country-Locations-en.csv",
            )
        )
        df_network = pd.concat([df_ipv4, df_ipv6])

        df_network["geoname_id"] = (
            df_network["geoname_id"].replace({np.nan: -1.0}).astype("Int64")
        )
        df_network["registered_country_geoname_id"] = (
            df_network["registered_country_geoname_id"]
            .replace({np.nan: -1.0})
            .astype("Int64")
        )
        df_network["represented_country_geoname_id"] = (
            df_network["represented_country_geoname_id"]
            .replace({np.nan: -1.0})
            .astype("Int64")
        )

        # save to local
        df_network.to_csv(
            self.GEOIP2_NETWORK_OUTPUT_PATH, index=False, header=False, sep="\t"
        )
        df_country_geoname_id.to_csv(
            self.COUNTRY_GEONAME_ID_OUTPUT_PATH, index=False, header=False, sep="\t"
        )
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
            cur = conn.cursor()
            # insert data to tables
            with open(self.GEOIP2_NETWORK_OUTPUT_PATH, "r") as f:
                cur.copy_from(
                    f, "geoip2_network_{date}".format(date=options["date"]), sep="\t"
                )

            print("insert geo2ip data successfully!")

            with open(self.COUNTRY_GEONAME_ID_OUTPUT_PATH, "r") as f:
                cur.copy_from(
                    f,
                    "country_geoname_id_{date}".format(date=options["date"]),
                    sep="\t",
                )

            print("insert country_geoname_id data successfully!")

            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
            print("Insert data successfully........")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            if conn is not None:
                conn.close()
