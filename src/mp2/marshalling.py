import json
from enum import Enum
from functools import wraps
from time import time
from dataclasses import asdict

from src.mp2.constants import MEMBER_ID, CURRENT_MEMBERS, DATA, INCARNATION, SUSPICION_ENABLED

def current_member_list_packet(member_list: list, sus_enabled=False: bool) -> str:
    """
    Creates a JSON packet containing the current members and their incarnation numbers,
    along with whether suspicion is enabled.
    """
    member_data = []
    for member in member_list:
        member_data.append({MEMBER_ID: member[MEMBER_ID], INCARNATION: member[INCARNATION]})
    packet = {CURRENT_MEMBERS: member_data, SUSPICION_ENABLED: sus_enabled}
    return json.dumps(packet)

def request_join_packet(id: int) -> str:
    """
    Creates a JSON packet to request joining the network with the given member id.
    """
    packet = {MEMBER_ID: id}
    return json.dumps(packet)

def ack_packet(id: int, data: str) -> str:
    
    """
    Creates a JSON acknowledgment packet containing the member id and event data.
    """
    
    packet = {MEMBER_ID: id, DATA: data}
    return json.dumps(packet)

def decode_message(message: str) -> dict:
    
    """
    Decodes a JSON message into a dictionary.
    """
    
    message_dict = json.loads(message)
    return message_dict
