from dataclasses import dataclass

INTRODUCER_PORT = 12346
FAILURE_DETECTOR_PORT = 12347
INTRODUCER_ID = 1



@dataclass
class Member:
    machine_id: int
    time_stamp: float