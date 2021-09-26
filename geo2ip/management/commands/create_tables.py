import logging

import psycopg2
from django.core.management.base import BaseCommand, CommandError


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Create tables geoip2_network, country_geoname_id, top_10_ip_range_country"

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

        """create tables in the PostgreSQL database"""
        commands = (
            """
            create table if not exists geoip2_network_{date} (
                  network VARCHAR(255) not null,
                  geoname_id BIGINT,
                  registered_country_geoname_id BIGINT,
                  represented_country_geoname_id BIGINT,
                  is_anonymous_proxy bool,
                  is_satellite_provider bool
            )
            """,
            """
            create table if not exists country_geoname_id_{date} (
                geoname_id BIGINT,
                locale_code VARCHAR(50),
                continent_code VARCHAR(50),
                continent_name VARCHAR(50),
                country_iso_code VARCHAR(50),
                country_name VARCHAR(50),
                is_in_european_union bool
            )
            """,
            """
            create table if not exists top_10_ip_range_country_{date} (
                country_name VARCHAR(50)
            )
            """,
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
            # create table one by one
            for command in commands:
                cur.execute(command.format(date=options["date"]))
            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
            print("Table created successfully........")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            if conn is not None:
                conn.close()
