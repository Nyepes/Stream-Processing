import json
from enum import Enum
from functools import wraps
from time import time
from dataclasses import asdict



class MessageType(Enum):
    JOIN = 1
    ACK = 2

def create_member_list(member_list):
    packet = {"members": member_list}
    return json.dumps(packet, default=asdict)

def create_join_message(id):
    packet = {"id": id, "timestamp": time()}
    return json.dumps(packet,default=asdict)

def create_ack_message(id, data):
    packet = {"id": id, "data": data}
    return json.dumps(packet, default=asdict)

def decode_message(message):
    message_dict = json.loads(message)
    return message_dict