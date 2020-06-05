# kitana
Partition management tool

# How to run
  - `git clone https://github.com/msklnko/kitana.git`
  - `go install .`
  - `kitana`

# Usage
  
  __Available Commands:__
   - cmt:         Add comment to provided table in supported format [GM:C:T:R:Rc]
   - daemon:      Run partitioning in daemon
   - help:        Help about any command
   - prt:         Used either to obtain information about information_schema.partitions
   - show:        Show all tables
  
  __Show partitions:__
  - `kitana show database`
    - -c - only with comment
    - -p only partitionised
    - -d with comment definition
  
  __Add comment:__
  - `kitana cmt database.table [GM:createdAt:ml:b:4] -s`
   - -s - show create table
  
  __Drop partition:__
  - `kitana prt status database.table`
  - `kitana prt drop database.table part202006 -s`
  
  __Add partition:__
  - `kitana prt add database.table part202008 1598961600 -s`
  
  __Run in daemon:__
  - `kitana daemon`
