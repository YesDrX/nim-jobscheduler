import db_connector/db_sqlite

const DefaultDbPath* = "jobscheduler.db"

proc openDatabase*(dbPath: string = DefaultDbPath): DbConn =
  ## Open database connection, create if doesn't exist
  result = open(dbPath, "", "", "")
  
  # Enable foreign keys
  result.exec(sql"PRAGMA foreign_keys = ON")

export DbConn
