"""Protocol for chat server - Computação Distribuida Assignment 1."""
from datetime import datetime
from socket import socket
import json
import pickle
import xml.etree.ElementTree as ET

JSON = 0
XML = 1
PICKLE = 2

class Message:
    """Message Type."""
    def __init__(self,command) -> None:
        self.command = command
    
class SubscribeMessage(Message):
    """Message to subscribe to a topic."""
    def __init__(self, topic: str, format: str):
        super().__init__("subscribe")
        self.topic = topic
        self.format = format
    
    def to_json(self):
        data = {"command": self.command, "topic": self.topic, "format": self.format}
        return json.dumps(data)
    
    def to_pickle(self):
        data = {"command": self.command, "topic": self.topic, "format": self.format}
        return pickle.dumps(data)
    
    def to_xml(self):
        data = f'<?xml version="1.0"?><data command="{self.command}" topic="{self.topic} format="{self.format}"></data>'
        return data


class PublishMessage(Message):
    """Message to publish in some topic."""
    def __init__(self, topic: str, value: str):
        super().__init__("publish")
        self.topic = topic
        self.value = value

    def to_json(self):
        data = {"command": self.command, "topic": self.topic, "value": self.value}
        return json.dumps(data)
    
    def to_pickle(self):
        data = {"command": self.command, "topic": self.topic, "value": self.value}
        return pickle.dumps(data)
    
    def to_xml(self):
        data = f'<?xml version="1.0"?><data command="{self.command}" topic="{self.topic}" value="{self.value}"></data>'
        return data
    

class AskTopicsMessage(Message):
    """Message to to list topics."""
    def __init__(self):
        super().__init__("ask_topics")

    def to_json(self):
        data = {"command": self.command}
        return json.dumps(data)
    
    def to_pickle(self):
        data = {"command": self.command}
        return pickle.dumps(data)
    
    def to_xml(self):
        data = f'<?xml version="1.0"?><data command="{self.command}"></data>'
        return data
    
    
class ListTopicsMessage(Message):
    """Message to to list topics."""
    def __init__(self, topics):
        super().__init__("list_topics")
        self.topics = topics

    def to_json(self):
        data = {"command": self.command}
        return json.dumps(data)
    
    def to_pickle(self):
        data = {"command": self.command}
        return pickle.dumps(data)
    
    def to_xml(self):
        data = f'<?xml version="1.0"?><data command="{self.command}"></data>'
        return data
    
    
class CancelMessage(Message):
    """Message to rcancel subscription to a topic."""
    def __init__(self, topic: str):
        super().__init__("cancel")
        self.topic=topic

    def to_json(self):
        data = {"command": self.command, "topic": self.topic}
        return json.dumps(data)
    
    def to_pickle(self):
        data = {"command": self.command, "topic": self.topic}
        return pickle.dumps(data)
    
    def to_xml(self):
        data = f'<?xml version="1.0"?><data command="{self.command}" topic="{self.topic}"></data>'
        return data
    

class Protocol:
    """Computação Distribuida Protocol."""
    @classmethod
    def subscribe(cls, topic: str, format: str) -> SubscribeMessage:
        """Creates a RegisterMessage object."""
        return SubscribeMessage(topic, format)

    @classmethod
    def publish(cls, topic: str, value) -> PublishMessage:
        """Creates a JoinMessage object."""
        return PublishMessage(topic, value)

    @classmethod
    def cancel(cls, topic: str) -> CancelMessage:
        """Creates a TextMessage object."""
        return CancelMessage(topic)
    
    @classmethod
    def ask_topics(cls) -> AskTopicsMessage:
        return AskTopicsMessage()
    
    @classmethod
    def list_topics(cls) -> ListTopicsMessage:
        return ListTopicsMessage()

    @classmethod
    def send_msg(cls, connection: socket, msg: Message, format: int):
        """Sends through a connection a Message object."""
        Message_final = None
        testsize = 0

        if format == None:
            format = JSON

        if format == JSON:
            message = json.dumps(msg.__dict__).encode("utf-8")
            size = len(message)
            formatBytes = format.to_bytes(1, "big")
            testsize = len(formatBytes)
            header = size.to_bytes(2, "big")
            Message_final = formatBytes + header + message

        elif format == XML:
            message = msg.to_xml().encode("utf-8")
            size = len(message)
            formatBytes = format.to_bytes(1, "big")
            testsize = len(formatBytes)
            header = size.to_bytes(2, "big")
            Message_final = formatBytes + header + message

        elif format == PICKLE:
            message = msg.to_pickle()
            size = len(message)
            formatBytes = format.to_bytes(1, "big")
            testsize = len(formatBytes)
            header = size.to_bytes(2, "big")
            Message_final = formatBytes + header + message

        if Message_final != None and len(Message_final) > testsize:
            connection.send(Message_final)
            

    #ALTERAR recv_mmsg
    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""

        print("entrou no recv_msg")
        format = int.from_bytes(connection.recv(1), "big")
        print("passou do format")
        size = int.from_bytes(connection.recv(2), "big")
        print("passou do size")

        print("recv_message protocol")
        print("format: ", format)
        print("size: ", size)

        if size == 0:
            return None
        
        if format == JSON:
            msg = json.loads(connection.recv(size).decode("utf-8"))

        elif format == XML:
            root = ET.fromstring(connection.recv(size).decode("utf-8"))
            msg = {}
            for node in root.keys():
                msg[node] = root.get(node)
            print("message from xml: " , msg)

        elif format == PICKLE:
            msg = pickle.loads(connection.recv(size)) 
        else:
            pass

        if msg["command"] == "subscribe":
            return cls.subscribe(msg.get("topic"), msg.get("format"))
        
        elif msg["command"] == "publish":
            return cls.publish(msg.get("topic"), msg.get("value"))
        
        elif msg["command"] == "cancel":
            return cls.cancel(msg.get("topic"))
        
        elif msg["command"] == "ask_topics":
            return cls.ask_topics()
        
        else:
            return None


class ProtocolBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")