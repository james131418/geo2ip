import logging

import psycopg2
from django.core.management.base import BaseCommand, CommandError


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Delete tables geoip2_network, country_geoname_id, top_10_ip_range_country"

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
            drop table if exists geoip2_network_{date}
            """,
            """
            drop table if exists country_geoname_id_{date}
            """,
            """
            drop table if exists top_10_ip_range_country_{date}
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
            print("Table deleted successfully........")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            if conn is not None:
                conn.close()
