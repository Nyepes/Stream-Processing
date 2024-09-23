import json
from enum import Enum
from functools import wraps
from time import time
from dataclasses import asdict

from src.mp2.constants import MEMBER_ID, CURRENT_MEMBERS, DATA



def current_member_list_packet(member_list):
    packet = {CURRENT_MEMBERS: member_list}
    return json.dumps(packet, default=asdict)

def request_join_packet(id):
    packet = {MEMBER_ID: id}
    return json.dumps(packet, default=asdict)

def ack_packet(id, data):
    packet = {MEMBER_ID: id, DATA: data}
    return json.dumps(packet, default=asdict)

def decode_message(message):
    message_dict = json.loads(message)
    return message_dict