"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from typing import Any
import socket
import selectors
from .protocol import Protocol


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self._type = _type
        self.format = 0
        self.host = 'localhost'
        self.port = 5000
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
    

    def push(self, value):
        """Sends data to broker."""
        msg_to_pub = Protocol.publish(self.topic, value)
        print('Middleware Sending: ', msg_to_pub)
        Protocol.send_msg(self.sock, msg_to_pub, self.format)

    def pull(self) -> (str, Any):
        """Receives (topic, data) from broker.
        Should BLOCK the consumer!"""

        message = Protocol.recv_msg(self.sock)
        print('Middleware Received: ', message)

        if message is None: 
            print("entrou no if message is None")
            return None
        
        print("não entrou no if message is none")
        return (message.topic, message.value)

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        msg = Protocol.ask_topics()
        Protocol.send_msg(self.sock, msg, self.format)

    def cancel(self):
        """Cancel subscription."""
        msg = Protocol.cancel(self.topic)
        Protocol.send_msg(self.sock, msg, self.format)

class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.format = 0
        if _type == MiddlewareType.CONSUMER:
            Protocol.send_msg(self.sock, Protocol.subscribe(topic, self.format), self.format)

class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.format = 1
        if _type == MiddlewareType.CONSUMER:
            Protocol.send_msg(self.sock, Protocol.subscribe(topic, self.format), self.format)

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.format = 2
        if _type == MiddlewareType.CONSUMER:
            Protocol.send_msg(self.sock, Protocol.subscribe(topic, self.format), self.format)
