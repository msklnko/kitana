# kitana
Partition management tool

# How to run
  - `git clone https://github.com/msklnko/kitana.git`
  - `go install .`
  - `kitana`

# Usage
  
  __Available Commands:__
   - __cmt__:         Add comment to provided table in supported format [GM:C:T:R:Rc]
   - __daemon__:      Run partitioning in daemon
   - __help__:        Help about any command
   - __prt__:         Used either to obtain information about information_schema.partitions
   - __show__:        Show all tables
  
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
  
   Every 30 seconds application would check if all necessary partitions are created and old partitions processed in the specified way
  - `kitana daemon`
