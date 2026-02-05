import times, strutils
import ../db/db_macros
import db_connector/db_sqlite
import ../db/serialize

type
  Token* {.dbTable.} = object
    name* : string
    token* : string
    userId* : int
    # createdAt handled by macro
    expiresAt* : DateTime

proc getTokensByUserId*(db: DbConn, userId: int): seq[tuple[dbId: int, data: Token]] =
  return queryRowsToken(db, "userId = " & $userId)

proc getTokenOwnerId*(db: DbConn, tokenId: int): int =
  let res = queryRowsToken(db, "_dbID = " & $tokenId)
  if res.len > 0:
    return res[0].data.userId
  return 0

proc deleteToken*(db: DbConn, id: int) =
    deleteRowToken(db, id)

