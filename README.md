## What is db-recovery?
```
       ____                                                   
  ____/ / /_        ________  _________ _   _____  _______  __
 / __  / __ \______/ ___/ _ \/ ___/ __ \ | / / _ \/ ___/ / / /
/ /_/ / /_/ /_____/ /  /  __/ /__/ /_/ / |/ /  __/ /  / /_/ / 
\__,_/_.___/     /_/   \___/\___/\____/|___/\___/_/   \__, /  
                                                     /____/
```

db-recovery is a tool for recovering MySQL data. It is used in scenarios where the database has no backup or binlog. It can parse data files and redo/undo logs to recover data.

- Recovery from data file

  Parse the contents of the recycle bin（PAGE_FREE） in the index page in the MySQL InnoDB data file, and generate the corresponding replace into statement.The index page is only the stored column data, and the corresponding column type needs to be obtained from the data dictionary to be parsed.

- Recovery from redo log

  Parsing the redo log is actually in order to parse the undo log in the redo, through the undo log we can get the data before modification, and then parse the data to generate the corresponding update statement.


## Quick start

1.clone the db-recovery repository
- git clone https://github.com/zbdba/db-recovery.git

2.Install the Golang environment.

- step1: Install go1.13 or later version.

- step2: Set the environment variable

  export GO111MODULE=on
  
  export GOPROXY=https://goproxy.io

3.Make the db-recovery
  
- cd db-recovery && make

4.Use the db-recovery help

```
[zbdba@zbdba db-recovery]$ ./bin/db-recovery
A simple command line client for github.com/zbdba/db-recovery.

Usage:
  github.com/zbdba/db-recovery [command]

Available Commands:
  help        Help about any command
  recovery    recovery related commands
  version     Print version info

Flags:
      --LogLevel string   set the log level. (default "DEBUG")
      --LogPath string    set the log file path. (default "/tmp")
      --OpType string     The OpType can be RecoveryData,RecoveryStruct,PrintData.
  -h, --help              help for github.com/zbdba/db-recovery

Use "github.com/zbdba/db-recovery [command] --help" for more information about a command.
```

You identify the command to get help

```
[zbdba@zbdba db-recovery]$ ./bin/db-recovery recovery --help
recovery related commands

Usage:
  github.com/zbdba/db-recovery recovery [command]

Available Commands:
  FromDataFile recovery from data file
  FromRedoFile recovery from redo file

Flags:
  -h, --help   help for recovery

Global Flags:
      --LogLevel string   set the log level. (default "DEBUG")
      --LogPath string    set the log file path. (default "/tmp")
      --OpType string     The OpType can be RecoveryData,RecoveryStruct,PrintData.

Use "github.com/zbdba/db-recovery recovery [command] --help" for more information about a command.
```

5.Example for db-recovery

- Recovery table type_test.test5 from MySQL InnoDB data file.
```
[root@zbdba db-recovery]# ./bin/db-recovery recovery FromDataFile \
--DBName="type_test" \
--SysDataFile="/data/mysql3322/data/ibdata1" \
--TableDataFile="/data/mysql3322/data/type_test/test5.ibd" \
--TableName="test5" \
--OpType="RecoveryData" 
```

- Recovery table type_test.test5 from MySQL InnoDB redo file.

```
./bin/db-recovery recovery FromRedoFile  \
--RedoFile="/data/mysql3322/data/ib_logfile0" \
--SysDataFile="/data/mysql3322/data/ibdata1" \
--DBName="type_test" \
--TableName="test5”
```

## Roadmap
- Support MySQL 8.0
- Support analysis of data files and redo log files
- Support recovery table structure
- Support compress or encrypt index page

## License
db-recovery is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Contributing
Contributions are welcomed and greatly appreciated.

## Contact
mail:875825800 at qq.com

## Acknowledgments
Thanks  [undrop-for-innodb](https://github.com/twindb/undrop-for-innodb) and [innodb_ruby](https://github.com/jeremycole/innodb_ruby) for giving me some inspiration and reference.