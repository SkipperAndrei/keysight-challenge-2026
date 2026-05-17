# Application architecture

The application requires the presence of csv file with the name **config.csv** in the **netem** folder of the project. 
This is where all the pattern configurations, drop rates, duplicate rates and delay times will be stored and parsed by the main thread.
The format of the file is the following:
```
  pattern,pq_id,drop_rate,dup_rate,delay_us
```
> **Note1**: drop_rate and dup_rate are parsed as percentages, so they require values in the [0, 1] interval.
> **Note2**: the pattern must be written in hexadecimal as 12 bytes, separated by `:`.
> **Note3**: delay_us is in microseconds.

After parsing the file, the main thread will launch 2 producer threads,
one for each direction of the traffic, 2 writer threads, 1 for printing statistics in the terminal and the others are worker threads. The total number of threads is defined in the **run.sh** script.

Each packet received will be searched to see if it contains one of the patterns in the config file. The order of the patterns in the file matters as the earlier they are declared, the bigger priority they will have. If the packet doesn't have any pattern, it will be sent to the default profile queue. In order to avoid polling multiple queues, the program has one big shared queue and when a packet is appended to it, it will receive a label with the id of it's profile queue.

For efficiency, since threads need sequential access to the queue, each worker collects packets in batches. The number of packets extracted is the minimum between the number of packets left in the queue and 32. For dequeing we use the function **rte_ring_dequeue_burst**, which has a lockless implementation, working on atomic compare and swap operations, which reduce the overhead of a traditional lock.

# Building
The main parts of the build process is setting up the run.sh script in the **netem/** folder.
The command has the following arguments:
``` 
  ./build/netem --no-huge -l <interval_of_thread_index_values> -n 1 -m <MB of RAM> --vdev 'net_pcap0,rx_pcap=<Absolute path to input .pcap file>,,infinite_rx=0' --vdev 'net_pcap1,tx_pcap=<Absolute path to output .pcap file>'

```

Example command: 
```
  ./build/netem --no-huge -l 0-7 -n 1 -m 100 --vdev 'net_pcap0,rx_pcap=/home/student/input.pcap,,infinite_rx=0' --vdev 'net_pcap1,tx_pcap=/home/student/output.pcap'
```

The application runs inside a Docker container, but thankfully all the commands to start everything are placed in the **auto_check.sh** bash script.

# Limitations:
The packet queue can only hold 4096 packets at the same time :(.
