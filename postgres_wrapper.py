import os
import logging
import psycopg2
from psycopg2 import sql
from dataclasses import dataclass

logging.basicConfig(
    filename="pose_extractor.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

@dataclass
class VideoProcessingResultFields:
    """
    simple dataclass for storing all required fields for table video_processing_results
    """
    video_filename: str
    duration_seconds: float = 0.0
    resolution: str = "N/A"
    codec: str = "N/A"
    frame_rate: float = 0.0
    corrupted: bool = None
    pose_file_path: str = "N/A"


class PostgresWrapper:
    """
    wrapper class for handling postgreSQL
    """
    def __init__(self):
        self.connection = None
        self.db_connect()

    def db_connect(self):
        """
        create a connection to the database
        :return: N/A
        """
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=os.getenv("POSTGRES_PORT", 5432),
                dbname=os.getenv("POSTGRES_DB", "pose_data"),
                user=os.getenv("POSTGRES_USER", "pose_user"),
                password=os.getenv("POSTGRES_PASSWORD", "pose_pass")
            )
            logging.info("successfully connected to database")
            self.connection = conn
        except psycopg2.OperationalError as e:
            logging.error(f"failed to connect to postgresSQL due to connection error: {e}")
        except psycopg2.InterfaceError as e:
            logging.error(f"failed to connect to postgresSQL due to interface error: {e}")
        except psycopg2.DatabaseError as e:
            logging.error(f"failed to connect to postgresSQL due to database error: {e}")
        except Exception as e:
            logging.error(f"failed to connect to postgresSQL - {e}")

    @property
    def is_connected(self) -> bool:
        """
        check whether connection is live and working
        :return: True if connection good, False otherwise
        """
        try:
            return self.connection is not None and self.connection.closed == 0
        except (psycopg2.Error, Exception):
            return False

    def db_insert(self, db_table: str, row_data: VideoProcessingResultFields) -> None:
        """
        insert into given table given row data
        :param db_table: table to insert inro
        :param row_data: row data as VideoProcessingResultFields dataclass
        :return: None
        """
        logging.info(f"inserting {row_data} into table {db_table}")

        try:
            with self.connection.cursor() as cursor:
                insert_query = sql.SQL(f"""
                        INSERT INTO {db_table} (
                            video_filename,
                            duration_seconds,
                            resolution,
                            codec,
                            frame_rate,
                            corrupted,
                            pose_file_path
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (video_filename) DO UPDATE SET
                            duration_seconds = EXCLUDED.duration_seconds,
                            resolution = EXCLUDED.resolution,
                            codec = EXCLUDED.codec,
                            frame_rate = EXCLUDED.frame_rate,
                            corrupted = EXCLUDED.corrupted,
                            pose_file_path = EXCLUDED.pose_file_path,
                            processed_at = CURRENT_TIMESTAMP
                        """)

                cursor.execute(insert_query, (
                    row_data.video_filename,
                    row_data.duration_seconds,
                    row_data.resolution,
                    row_data.codec,
                    row_data.frame_rate,
                    row_data.corrupted,
                    row_data.pose_file_path
                ))

                self.connection.commit()
        except Exception as e:
            logging.error(f"Error inserting video processing result: {e}")
            self.connection.rollback()
        else:
            logging.info(f"finished inserting video processing result for {row_data.video_filename}")
