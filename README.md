```
usage: sync [-h] -s SOURCE -r REPLICA [-lf LOG_FILE] [-ll {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [-i INTERVAL]

Program for synchronizing and replicating source directory into a replica directory.

options:
  -h, --help            show this help message and exit
  -s SOURCE, --source SOURCE
                        Source directory path
  -r REPLICA, --replica REPLICA
                        Replicated directory path
  -lf LOG_FILE, --log-file LOG_FILE
                        Log file path
  -ll {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Defaults to INFO
  -i INTERVAL, --interval INTERVAL
                        Sync interval in seconds or minutes (defaults to 1m)
```
