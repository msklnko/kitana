# kitana
Partition management tool

# How to run
  - `git clone https://github.com/msklnko/kitana.git`
  - `go install .`
  - `kitana`

# Usage
  
  Available Commands:
  cmt         Add comment to provided table in supported format [GM:C:T:R:Rc], >
  daemon      Run partitioning in daemon,
  help        Help about any command,
  prt         Used either to obtain information about information_schema.partitions,
  show        Show all tables
  
  
  Add comment:
  - `kitana cmt wallet_wtc.transactionStorno [GM:createdAt:ml:b:4] -s`
   -s - show create table
  
  Drop partition:
  - `kitana prt status wallet_wtc.transactionReferralPayout`
  - `kitana prt drop wallet_wtc.transactionReferralPayout part202006 -s`

