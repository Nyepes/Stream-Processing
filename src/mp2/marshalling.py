import json
from enum import Enum
from functools import wraps

class MessageType(Enum):
    JOIN_ACK = 1
    JOIN_REQUEST = 2
    ACK = 3 
    SUSPICION = 4
    FAIL = 5
    PING = 6

def message(message_type: MessageType):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result_dict = func(*args, **kwargs)
            merged_dict = {"type": message_type.value, **result_dict}
            json_data = json.dumps(merged_dict)
            return json_data
        return wrapper
    return decorator

@message(MessageType.JOIN_ACK)
def create_member_list(member_list):
    return {"members": member_list}

@message(MessageType.JOIN_REQUEST)
def create_join_message(id):
    return {"id": id}

@message(MessageType.ACK)
def ack(id):
    return {"id": id}

@message(MessageType.FAIL)
def created_message_failed(id):
    return {"id": id}

@message(MessageType.PING)
def created_message_failed(id):
    return {"id": id}

def decode_message(message):
    message_dict = json.loads(message)
    message_type = MessageType(message_dict["type"])
    message_dict["type"] = message_type
    return message_dict