import times, options, strutils
import ../db/db_macros
import db_connector/db_sqlite
import ../db/serialize

# Define User object using new dbTable macro style
type
  User* {.dbTable.} = object
    username* : string
    passwordHash* : string
    # createdAt handled by macro

export User

proc getUserByUsername*(db: DbConn, username: string): Option[tuple[dbId: int, data: User]] =
  let users = queryRowsUser(db, "username = '" & username.replace("'", "''") & "'")
  if users.len > 0:
    return some(users[0])
  return none(tuple[dbId: int, data: User])

proc getUserById*(db: DbConn, id: int): Option[tuple[dbId: int, data: User]] =
  let users = queryRowsUser(db, "_dbID = " & $id)
  if users.len > 0:
    return some(users[0])
  return none(tuple[dbId: int, data: User])

proc getAllUsers*(db: DbConn): seq[tuple[dbId: int, data: User]] =
  return queryRowsUser(db, "1=1 ORDER BY username")

proc updateUserPassword*(db: DbConn, id: int, passwordHash: string) =
  db.exec(sql"UPDATE UserTable SET passwordHash = ? WHERE _dbID = ?", passwordHash, id)

proc deleteUser*(db: DbConn, id: int) =
    deleteRowUser(db, id)

