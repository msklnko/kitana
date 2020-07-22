# kitana
Partition management tool

# How to run
  - `git clone https://github.com/msklnko/kitana.git`
  -  Configure`./settings.yaml`
  - `go install .`
  - `kitana`

# Usage
  
  __Available Commands:__
- __comment__:     Add comment to provided table in supported format [GM:C:T:R:Rc] (example: `kitana comment database.table [GM:createdAt:dl:d:4]`)
- __daemon__:      Run partitioning in daemon (example: `kitana daemon database`)
- __help__:        Help about any command
- __index__:       Update primary index (example: `kitana index database.table column1,column2`)
- __partition__:   Used either to obtain information about partitions
- __show__:        Show all tables from database (example: `kitana show database`)
- __test__:        Tests given string as table comment

  
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
