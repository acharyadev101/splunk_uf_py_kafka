from flask import Flask
from confluent_kafka import Producer
import socket
import threading
import signal
import sys
import traceback

app = Flask(__name__)

# Configure Kafka producer
config = {'bootstrap.servers': 'localhost:9092'}  # replace with your Kafka server address
producer = Producer(config)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def tcp_server():
    try:
        serversocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        serversocket.bind(('0.0.0.0',42069))
        serversocket.listen(5)
        while True:
            try:
                (clientsocket) = serversocket.accept()
                rd = clientsocket.recv(5000).decode()
                if rd:
                    print(f'{rd}')
                    producer.produce('mytopic', rd, callback=delivery_report)  # replace 'mytopic' with your topic
                    producer.poll(0)
                clientsocket.close()
            except Exception as e:
                print(f"Error occurred while handling clientsocket: {e}")
                traceback.print_exc()

    except Exception as e:
        print(f"Error occurred while setting up server: {e}")
        traceback.print_exc()

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    try:
        server_thread = threading.Thread(target=tcp_server)
        server_thread.start()

        signal.signal(signal.SIGTERM, lambda signnum, stack_frame: sys.exit(1))

        app.run(host='0.0.0.0', port=8080)
    except Exception as e:
        print(f"Error occurred while starting server: {e}")
        traceback.print_exc()