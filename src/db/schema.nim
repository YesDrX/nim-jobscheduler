import db_connector/db_sqlite
import ../models/user
import ../models/task
import ../models/job
import ../models/execution
import ../models/token

proc initDb*(db: DbConn) =
  createUserTable(db)
  createTaskTable(db)
  createJobTable(db)
  createExecutionTable(db)
  createTokenTable(db)
