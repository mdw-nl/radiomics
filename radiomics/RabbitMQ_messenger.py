import json
import logging
from pathlib import Path

from config_handler import Config
from consumer import Consumer

logger = logging.getLogger(__name__)

"""This messenger class is made to build and send messages based on the arguments: data_folder, which is the directory of the folder path with
the data and the queue. The queue is based on the name of the next rabbbitmq name, make sure that the queue name als equals the name of that
part of the config file"""


class messenger:
    def __init__(self):
        self.message_folder = "messages/radiomics_messages"
        self.output_file = "message.json"

    def create_message_next_queue(self, queue, data_folder):
        # Create the folder for the next message
        output_file_path = Path(self.message_folder) / self.output_file
        output_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Create the message
        config = {"folder_path": data_folder, "action": queue}

        with output_file_path.open("w") as file:
            json.dump(config, file, indent=2)

        logger.info("RabbitMQ message created at: %s", output_file_path)

        # Send the message
        rabbitMQ_config_radiomics = Config(queue)
        radiomics = Consumer(rmq_config=rabbitMQ_config_radiomics)
        radiomics.open_connection_rmq()
        radiomics.send_message(self.message_folder)

        logger.info("Send the data: %s to: %s", data_folder, queue)
