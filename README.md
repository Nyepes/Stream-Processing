README.md

To use the system we have to run 

```bash
./run.sh start <VM_ID>
```

This starts both the distributed file system and the underlying failure detection system. The latter is from Failure Detector (check Failure Detector README.md for reference) and helps us distribute files to the appropriate node when failure occurs. 

To create a file we run the command

```bash
./run.sh create <local> <server>
```

Note that we expect the local file first and then the server destination second. Also with create we will automatically add the content of the local file to the DFS server destination. Also it will automatically send the file to the next K - 1 successors to keep replicas. Finally, we can only create a file with name <server> once, any latter calls will gracefully fail. 

To append to a file we run the command

```bash
./run.sh append <local> <server>
```

Again we use the same conversion as create for the local server relationship. Now when we append we send the content of the local file to some replica determined by a function we have created. Its purpose is to avoid high load on the head replica. Also we guarantee casual ordering from a node appending two file to the same server file for example

To merge we run the command

```bash
./run.sh merge <server>
```

The command will make all replicas have the same data. Thus ensuring consistency. 

To get a file we run the command

```bash
./run.sh get <local> <server>
```

This command will get the server file and store it in the local file. 

Finally we have the command multiappend by running the command

```bash
./run.sh multiappend <server> <VM_ID_1>  … <VM_ID_N> <local_1> … <local_3>

The multiappend command appends the file respective to the id provided to the server file. 

To output the nodes containing the file <FILE> we run the command 

```bash
./run.sh ls <FILE>
``` 

The command will essentially output all the nodes which own a replica of <FILE>

```bash
./run.sh list_mem
```

To output the files stored at your vm we run the command

```bash
./run.sh store
```



