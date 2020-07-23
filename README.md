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
  - `kitana comment database.table [GM:createdAt:ml:b:4] -s`
   - -s - show create table
   
  __Test comment:__
  - `kitana test [GM:createdAt:ml:b:4]`
  
  __Partition:__
    __Drop partition:__
    - `kitana partition status database.table`
    - `kitana partition drop database.table part202006 -s`
  
    __Add partition:__
    - `kitana partition add database.table part202008 1598961600 -s`
   
    __Actualize partitions:__
    - `kitana partition actualize database.table` 
     - -d - Drop partitions interval (default 500ms)
     - -f - Force delete all expired partitions (by default drop partitions would be one by one with default interval 500ms)
     
    __Partition table:__
    - `kitana partition create database.table`
     - -c - Number of partitions to create in advance, default = 3 (default 3)
    
    __Partitions status:__
    - `kitana partition status database.table`
  
  __Run in daemon:__
  - `kitana daemon database`
   - -d - Daemon drop partitions interval (default 500ms)
   - -f - Force delete all expired partitions (by default drop partitions would be one by one with default interval 500ms)
   - -r - Daemon refresh interval (default 30s). Means kitana would be executed every 30 seconds 
  
   Every 30 seconds application would check if all necessary partitions are created and old partitions processed in the specified way
  - `kitana daemon`
