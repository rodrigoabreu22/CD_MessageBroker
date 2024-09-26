"""Message Broker"""
import enum
import socket
from typing import Dict, List, Any, Tuple
import selectors
import json
import logging
from .protocol import Protocol

logging.basicConfig(filename="broker.log", level=logging.DEBUG)


class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self.topics = {} # stores last value of each topic
        self.subscribers = {} # stores all subscribers of each topic

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sel = selectors.DefaultSelector()
        
        self.socket.bind((self._host, self._port))
        self.socket.listen(100)
        self.sel.register(self.socket, selectors.EVENT_READ, self.accept)
    

    def accept(self, sock, mask):
        conn, addr = sock.accept() 
        print('accepted', conn, 'from', addr)
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)
        
    def read(self, conn, mask):

        try:
            data = Protocol.recv_msg(conn)

            if data:
                print('Received: ', data)
                print('Command: ', data.command)
                
                logging.info(f"Received message: {data}")
                logging.info(f"Command: {data.command}")
                command = data.command

                if command == "subscribe":
                    self.subscribe(data.topic, conn, int(data.format))

                elif command == "publish":
                    logging.info(f"Received message to publish: {data.topic} {data.value}")
                    self.put_topic(data.topic, data.value)

                elif command == "cancel":
                    self.unsubscribe(data.topic, conn)

                elif command == "ask_topics":
                    self.list_topics(conn)

                else:
                    print("Unknown command")
            else:
                print("Connection closed")
                for i in self.subscribers:
                    for j in self.subscribers[i]:
                        if j[0] == conn:
                            self.subscribers[i].remove(j)
                            break
                self.sel.unregister(conn)
                conn.close()

        except ConnectionResetError:
            print("Connection closed")
            for i in self.subscribers:
                for j in self.subscribers[i]:
                    if j[0] == conn:
                        self.subscribers[i].remove(j)
                        break
            self.sel.unregister(conn)
            conn.close()

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        topics_list = []
        for topic in self.topics:
            if self.topics[topic] is not None:
                topics_list.append(topic)
        
        print(topics_list)
        return topics_list

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic not in self.topics:
            return None
        
        print("Topic: ", topic, "\tValue: ", self.topics.get(topic))
        return self.topics.get(topic)

    def put_topic(self, topic, value):
        """Store in topic the value."""

        self.topics[topic] = value

        for topic2 in self.topics:
            if topic==topic2 or topic.startswith(topic2 + "/"):
                if topic2 in self.subscribers:
                    for sub in self.subscribers[topic2]:
                        logging.info(f"Sending message to {sub[0]}")
                        Protocol.send_msg(sub[0], Protocol.publish(topic2, value), sub[1])



    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""

        if topic in self.subscribers:
            return self.subscribers[topic]

        return []   
    


    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""

        if topic not in self.topics:
            self.topics[topic] = None
            self.subscribers[topic] = [(address, _format)]

        elif topic not in self.subscribers:
            self.subscribers[topic] = [(address, _format)]

        elif (address, _format) not in self.subscribers[topic]:
            self.subscribers[topic].append((address, _format))

        else:
            print("Error: Already subscribed to topic: ", topic)
            

        if self.topics[topic] is not None: 
            Protocol.send_msg(address, Protocol.publish(topic, self.topics[topic]), _format)

        print("Subscribed to topic: ", topic)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        if topic in self.subscribers:
            for sub in self.subscribers[topic]:
                if  address==sub[0]:
                    self.subscribers[topic].remove(sub)
                    print("Unsubscribed from topic: ", topic)
                    return

    def run(self):
        """Run until canceled."""

        while not self.canceled:
            events = self.sel.select(timeout=None)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
