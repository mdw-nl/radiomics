import pika
import logging
import concurrent.futures
import time
import threading
import os
import json

class Consumer:
    def __init__(self, rmq_config):
        self.connection_rmq = None
        self.channel = None
        self.config_dict_rmq = rmq_config.config
        self.db = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self._connected = False
        self.retry_attempt = 5

    def open_connection_rmq(self):
        """Establish connection"""
        host, port, user, pwd = self.config_dict_rmq["host"], self.config_dict_rmq["port"] \
            , self.config_dict_rmq["username"], self.config_dict_rmq["password"]

        connection_string = f"amqp://{user}:{pwd}@{host}:{port}/"
        connection = pika.BlockingConnection(pika.URLParameters(connection_string))
        self.connection_rmq = connection
        self._connected = True

    def reconnect(self):
        self.open_connection_rmq()
        self.create_channel()

    def create_channel(self):
        if not self.connection_rmq or self.connection_rmq.is_closed:
            self.open_connection_rmq()
        self.channel = self.connection_rmq.channel()

    def close_connection(self):
        """Close connection"""
        if self.connection_rmq:
            self.channel.close()
            self.connection_rmq.close()
            logging.info("RMQ Connection closed.")

    def check_queue_exists(self):
        """Check if the queue exists"""
        try:
            self.channel.queue_declare(queue=self.config_dict_rmq["queue_name"], passive=True)
            logging.info(f"Queue '{self.config_dict_rmq['queue_name']}' exists.")
        except pika.exceptions.ChannelClosedByBroker as e:
            logging.error(f"Queue '{self.config_dict_rmq['queue_name']}' does not exist.")
            raise
        except Exception as e:
            logging.error(f"An error occurred while checking the queue: {e}")
            raise e

    def start_consumer(self, callback):
        while True:
            i = 0
            self.channel.basic_consume(queue=self.config_dict_rmq["queue_name"],
                                       on_message_callback=lambda ch, method, properties, body: callback(ch, method,
                                                                                                         properties,
                                                                                                         body,
                                                                                                         self.executor),
                                       auto_ack=False)
            try:
                self.channel.start_consuming()
                break
            except KeyboardInterrupt:
                print("Consumer stopped by user.")
                break
            except Exception as e:

                logging.error(f"An error occurred while consuming messages: {e}")
                logging.error("Reconnecting to RabbitMQ...")
                while i < self.retry_attempt:
                    try:
                        i += 1
                        self.reconnect()
                        self.check_queue_exists()
                        self.start_consumer(callback)
                        break
                    except Exception as e:
                        logging.error(f"Reconnection attempt {i + 1} failed: {e}")
                        time.sleep(5)

        self.executor.shutdown(wait=True)
        self.close_connection()

    def send_message(self, folder_path):
        """Send message to queue"""
        if self.connection_rmq is None or self.connection_rmq.is_closed:
                self.open_connection_rmq()

        if self.channel is None or self.channel.is_closed:
            self.create_channel()        

        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            with open(file_path, 'r') as f:
                data = json.load(f)
            message_json = json.dumps(data)
            
            self.channel.basic_publish(
            exchange='',
            routing_key=self.config_dict_rmq["queue_name"],
            body=message_json
            )
            logging.info(f"Sent message: {file}")