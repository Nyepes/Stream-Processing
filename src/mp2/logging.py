
def log(line: str, id):
    """
    write a log message to a file. You must specify the id to determine which log file to write to
    as well as the message to write
    """
    with open(f"src/machine.{id}.log", "a") as log:
        log.write(f"{line}\n")
    