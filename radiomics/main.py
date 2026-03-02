import logging
import os
from config_handler import Config
from consumer import Consumer
from PostgresInterface import PostgresInterface
from global_var import QUERY_UID
from xnat_sender import SendDICOM
from radiomics_calculator import RadiomicsCalculator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()


def connect_db():
    """Connect to the PostgreSQL database."""
    postgres_config = Config("postgres")
    if postgres_config is None:
        raise Exception("Postgres config is None")
    config_dict_db = postgres_config.config
    host, port, user, pwd, db = config_dict_db["host"], config_dict_db["port"], \
        config_dict_db["username"], config_dict_db["password"], config_dict_db["db"]
    db = PostgresInterface(host=host, database=db, user=user, password=pwd, port=port)
    db.connect()
    logging.info("Connected to the database")
    return db


class RadiomicsPipeline:
    def __init__(self):
        self.db = connect_db()
        self.calculator = RadiomicsCalculator()
        self.xnat_sender = SendDICOM()

    def get_folder_from_db(self, study_uid):
        """Retrieve the study folder path from the database using the study UID."""
        result = self.db.fetch_one(QUERY_UID, params=(study_uid,))
        if result:
            file_path = result[0]
            return os.path.dirname(os.path.dirname(file_path))
        else:
            logging.error(f"No folder found in DB for study UID: {study_uid}")
            return None

    def run(self, ch, method, properties, body, executor):
        ch.basic_ack(delivery_tag=method.delivery_tag)

        study_uid = body.decode("utf-8").strip()
        logging.info(f"Received study UID: {study_uid}")

        data_folder = self.get_folder_from_db(study_uid)
        if not data_folder:
            logging.error(f"Could not resolve folder for study UID: {study_uid}")
            return

        try:
            csv_content, metadata, filename = self.calculator.run(data_folder)
            self.xnat_sender.upload_to_xnat(csv_content, metadata, filename)
            logging.info("Radiomics pipeline completed successfully.")

        except Exception as e:
            logging.error(f"An error occurred in the pipeline: {e}", exc_info=True)


if __name__ == '__main__':
    rabbitMQ_config = Config("radiomics")
    cons = Consumer(rmq_config=rabbitMQ_config)
    cons.open_connection_rmq()
    cons.create_channel()
    cons.channel.queue_declare(
        queue=rabbitMQ_config.config["queue_name"],
        durable=True
    )

    pipeline = RadiomicsPipeline()
    cons.start_consumer(callback=pipeline.run)