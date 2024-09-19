# CS425 MP1: Distributed Log Querier

This file outline how to run the distributed log querier implementation.

## Distributed Log Querier

1. Connect to each machine using ssh.

```bash
ssh <netid>@fa24-cs425-06<machineid>.cs.illinois.edu
```

Where machineid is a number 01 to 10 inclusive.

2. Clone repository 

```bash
git clone https://gitlab.engr.illinois.edu/nyepes2/g06.git
```
3. Build and add needed dependencies (on each machine)
./run.sh build


4. Start server (on each machine)

```bash
./run.sh dgrep_server <machineid> <flags>
```
`-t` adding this flag will create a server in test mode so that it will query the log file `test.log` (see Testing Section)

5. Start client (on any single machine)

```bash
./run.sh dgrep <flags> <command>
```

The client essentially works as a wrapper for grep. Therefore, <flags> should be valid grep flags and <command> should be a valid grep argument to match.

Below are some examples:

### Matching lines for raw string GET
```bash
./run.sh dgrep "GET"
```

Prints the actual matching raw strings

```
machine.1.log: ... GET ...
... GET ...
... GET ...
... GET ...
... GET ...
...
machine.10.log: ... GET ...
... GET ...
... GET ...
... GET ...
... GET ...
```

### Matching lines count for raw string GET
```bash
./run.sh dgrep -c "GET"
```

The -c flag prints the matching line count in each machine and the matching line count in the whole system.

```
machine.1.log: <num>
...
machine.10.log: <num>
TOTAL: <aggregate>
```

### Matching lines count for regex /product/\<num\>
```bash
./run.sh dgrep -c -P "/product/\d+"
```

The -P is used to allow use of PCRE regular expressions such as \d for digits

## Testing

1. Run generate_log.py (on each machine)

```bash
python3 generate_log.py
```

2. Run server.py (on each machine)

Just as previous section, but use -t flag to run grep on test.log

```bash
python3 server.py <machineide> -t
```

3. Run tests/test_accuracy.py (on any single machine)

```bash
python3 tests/test.py
```

