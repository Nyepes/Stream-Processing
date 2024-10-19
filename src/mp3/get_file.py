from src.shared.constants import HOSTS
"""
Script to run when calling ./run.sh list_mem
"""



def get_machines():
    machines = []
    with open("src/member_list.txt", "r") as member_list_file:
        for line in member_list_file:
            machine_id = int(line.strip())
            machines.append(machine_id)
    return machines

if __name__ == "__main__":
    
    file_name = int(sys.argv[1])
    output_file = int(sys.argv[2])
    machines = get_machines()

    value = generate_sha1(file_name)

    machine_with_file = value % 10 + 1

    id = os.environ["ID"]

    if (id < machine_with_file + REPLICATION_FACTOR and id >= machine_with_file):
        # Link ? Idk
        # TODO: dup2() for efficiency
    
    else:
        request_file(machine_with_file + id % REPLICATION_FACTOR, file_name, output_file)


