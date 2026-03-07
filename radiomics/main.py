import logging
from pathlib import Path

from config_handler import Config
from consumer import Consumer
from global_var import QUERY_UID, SEND_XNAT, SEND_POSTGRESS
from PostgresInterface import PostgresInterface
from radiomics_calculator import RadiomicsCalculator
from xnat_sender import SendDICOM
from radiomics_results_postgress import setup_radiomics_db, send_postgress

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def connect_db():
    """Connect to the PostgreSQL database."""
    postgres_config = Config("postgres")
    if postgres_config is None:
        raise Exception("Postgres config is None")
    config_dict_db = postgres_config.config
    host, port, user, pwd, db = (
        config_dict_db["host"],
        config_dict_db["port"],
        config_dict_db["username"],
        config_dict_db["password"],
        config_dict_db["db"],
    )
    db = PostgresInterface(host=host, database=db, user=user, password=pwd, port=port)
    db.connect()
    logger.info("Connected to the database")
    return db


class RadiomicsPipeline:
    def __init__(self):
        self.db = connect_db()
        self.calculator = RadiomicsCalculator()
        self.xnat_sender = SendDICOM()
        postgres_db = setup_radiomics_db()
        postgres_db.run(self.db)

    def get_folder_from_db(self, study_uid):
        """Retrieve the study folder path from the database using the study UID."""
        result = self.db.fetch_one(QUERY_UID, params=(study_uid,))
        if result:
            file_path = result[0]
            return Path(file_path).parent.parent
        logger.error("No folder found in DB for study UID: %s", study_uid)
        return None

    def run(self, ch, method, properties, body, executor):
        ch.basic_ack(delivery_tag=method.delivery_tag)

        study_uid = body.decode("utf-8").strip()
        logger.info("Received study UID: %s", study_uid)

        data_folder = self.get_folder_from_db(study_uid)
        if not data_folder:
            logger.error("Could not resolve folder for study UID: %s", study_uid)
            return

        try:
            csv_content, metadata, filename = self.calculator.run(data_folder)
            
            if SEND_POSTGRESS == True:
                send_postgress(self.db, csv_content, metadata)
            
            if SEND_XNAT == True:
                self.xnat_sender.upload_to_xnat(csv_content, metadata, filename)
            logger.info("Radiomics pipeline completed successfully.")

        except Exception:
            logger.exception("An error occurred in the pipeline.")


if __name__ == "__main__":
    rabbitMQ_config = Config("radiomics")
    cons = Consumer(rmq_config=rabbitMQ_config)
    cons.open_connection_rmq()
    cons.create_channel()
    cons.channel.queue_declare(queue=rabbitMQ_config.config["queue_name"], durable=True)

    pipeline = RadiomicsPipeline()
    cons.start_consumer(callback=pipeline.run)
