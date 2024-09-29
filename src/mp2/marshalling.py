import json
from enum import Enum
from functools import wraps
from time import time
from dataclasses import asdict

from src.mp2.constants import MEMBER_ID, CURRENT_MEMBERS, DATA, INCARNATION, SUSPICION_ENABLED



def current_member_list_packet(member_list, sus_enabled = False):
    member_data = []
    for member in member_list:
        member_data.append({MEMBER_ID: member[MEMBER_ID], INCARNATION: member[INCARNATION]})
    packet = {CURRENT_MEMBERS: member_data, SUSPICION_ENABLED: sus_enabled}
    return json.dumps(packet)

def request_join_packet(id):
    packet = {MEMBER_ID: id}
    return json.dumps(packet)

def ack_packet(id, data):
    packet = {MEMBER_ID: id, DATA: data}
    return json.dumps(packet)

def decode_message(message):
    message_dict = json.loads(message)
    return message_dict