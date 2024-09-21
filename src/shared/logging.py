
def remember_id(func):
    stored_id = None
    file = None
    def wrapper(line: str, id=None):
        nonlocal stored_id
        nonlocal file
        if id is not None:
            stored_id = id
            file = f"src/machine.{id}.log"
        if stored_id is None:
            raise ValueError("ID must be provided on the first call")
        return func(line, file)
    return wrapper

@remember_id
def log(line: str, file):
    """
    write a log message to a file. You must specify the id to determine which log file to write to
    as well as the message to write
    """
    with open(file, "a") as log:
        log.write(f"{line}\n")
    