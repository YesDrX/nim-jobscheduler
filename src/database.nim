import std/[macros, strformat, sequtils, times, sugar, strutils, tables]

import db_connector/db_sqlite
import ./[serialize, utils]

export db_sqlite, serialize, utils

# helpers
proc nimTypeToSqlType*(fieldType: string): string =
  case fieldType
  of "string":
    return "TEXT"
  of "int", "int64":
    return "INTEGER"
  of "float", "float64":
    return "REAL"
  of "bool":
    return "INTEGER"
  else:
    return "TEXT"

template objDefPragmaHelper*(code: untyped): untyped =
  #[
    This macro is to help the pragma on the object definition to define top level proc/methods. For example,
    if we defined a pragma named `dbTable` and use it like so,
    ```nim
    type
        StructName {.dbTable.} = object
            field1 : string
            field2 : int
    ```
    The dbTable macro will get a typedef rather than typesection as input. If we want to extend the StructName
    definition by adding some methods, we can't as those added code will live under the type section, rather than
    on the top level.
    Instead, we can generate some code like this
    ```nim
    type
        ExtendStructName = objDefPragmaHelper do:
            type
                StructName {.inject.} = object
                    field1 : string
                    field2 : int
            proc method1(self : StructName) : string =
                discard
            proc method2(self : StructName) : int =
                discard
            const ExtendStructNameConst = ""
            StructName
    ```
    Given the above code, StructName, method1, method2, ExtendStructNameConst will be defined on the top level,
    ]#
  code

proc getObjDefinitions*(objDef: NimNode): tuple[objName: string, fields: seq[
    tuple[fieldName, fieldType: string]]] =
  objDef.expectKind(nnkTypeDef)

  var objName: string
  var nameNode = objDef[0]
  if nameNode.kind == nnkPragmaExpr:
    nameNode = nameNode[0]

  case nameNode.kind
  of nnkIdent:
    objName = nameNode.repr
  of nnkPostfix:
    objName = nameNode[^1].repr
  else:
    raise newException(ValueError, "Invalid object definition: " &
        $nameNode.kind)

  # get object fields
  var fieldsDef = objDef[2]
  if fieldsDef.kind == nnkRefTy:
    fieldsDef = fieldsDef[0]
  fieldsDef.expectKind(nnkObjectTy)
  let recList = fieldsDef[2]
  recList.expectKind(nnkRecList)
  var fields: seq[tuple[fieldName, fieldType: string]]
  for identDef in recList:
    var fieldName, fieldType: string

    var fieldNameNode = identDef[0]
    if fieldNameNode.kind == nnkPragmaExpr:
      fieldNameNode = fieldNameNode[0]

    case fieldNameNode.kind
    of nnkIdent:
      fieldName = fieldNameNode.repr
    of nnkPostfix:
      fieldName = fieldNameNode[^1].repr
    else:
      raise newException(ValueError, "Invalid field definition")

    fieldType = identDef[1].repr
    fields.add((fieldName, fieldType))

  result.objName = objName
  result.fields = fields

# dbTable pragma
proc createTableProc*(objName: string, fields: seq[tuple[fieldName,
        fieldType: string]]): NimNode =
  let createTableStr = nnkCallStrLit.newTree(
      newIdentNode("sql"),
      newLit(fmt"""CREATE TABLE IF NOT EXISTS {objName}Table (_dbID INTEGER PRIMARY KEY AUTOINCREMENT, _dbTimestamp INTEGER, {fields.map(f => f.fieldName & " " & f.fieldType.nimTypeToSqlType).join(", ")})""")
  )
  let procName = nnkPostfix.newTree(newIdentNode("*"), newIdentNode(
          "createTable" & objName))
  let db = newIdentNode("db")
  result = quote do:
    proc `procName`(`db`: DbConn) =
      `db`.exec(`createTableStr`)

proc insertRowProc*(objName: string, fields: seq[tuple[fieldName,
    fieldType: string]]): NimNode =
  let insertRowStr = fmt"""INSERT INTO {objName}Table (_dbTimestamp, {fields.map(f => f.fieldName).join(", ")}) VALUES (?, {fields.map(f => "?").join(", ")})"""

  let procName = nnkPostfix.newTree(newIdentNode("*"), newIdentNode(
          "insertRow" & objName))
  let db = newIdentNode("db")
  let obj = newIdentNode("obj")
  let objType = newIdentNode(objName)

  proc genSerializedField(fieldName: string): NimNode =
    result = fmt"obj.{fieldName}.serialize()".parseStmt[0]
  let dbFields = fields.map(f => genSerializedField(f.fieldName))

  var dbCall = fmt"""db.tryInsertID(sql"{insertRowStr}", $times.now().toTime().toUnix())""".parseStmt[0]
  dbCall.add(dbFields)

  result = quote do:
    proc `procName`(`db`: DbConn, `obj`: `objType`): int =
      try:
        return `dbCall`.int
      except:
        utils.error "Error inserting row: " & getCurrentExceptionMsg()
        `db`.exec(sql"ROLLBACK")
        raise getCurrentException()

proc insertRowsProc*(objName: string, fields: seq[tuple[fieldName,
        fieldType: string]]): NimNode =
  let insertRowsStr = fmt"""INSERT INTO {objName}Table (_dbTimestamp, {fields.map(f => f.fieldName).join(", ")}) VALUES (?, {fields.map(f => "?").join(", ")})"""
  let procName = nnkPostfix.newTree(newIdentNode("*"), newIdentNode(
          "insertRows" & objName))
  let db = newIdentNode("db")
  let row = newIdentNode("row")
  let objs = newIdentNode("objs")
  let objType = newIdentNode(objName)

  proc genSerializedField(fieldName: string): NimNode =
    result = fmt"row.{fieldName}.serialize()".parseStmt[0]
  let dbFields = fields.map(f => genSerializedField(f.fieldName))
  var dbCall = fmt"""db.exec(sql"{insertRowsStr}", $times.now().toTime().toUnix())""".parseStmt[0]
  dbCall.add(dbFields)

  result = quote do:
    proc `procName`(`db`: DbConn, `objs`: seq[`objType`]) =
      try:
        for `row` in `objs`:
          `dbCall`
      except:
        utils.error "Error inserting rows: " & getCurrentExceptionMsg()
        `db`.exec(sql"ROLLBACK")
        raise getCurrentException()

proc updateRowProc*(objName: string, fields: seq[tuple[fieldName,
        fieldType: string]]): NimNode =
  let updateRowStr = fmt"""UPDATE {objName}Table SET _dbTimestamp = ?, {fields.map(f => f.fieldName & " = ?").join(", ")} WHERE _dbID = ?"""
  let procName = nnkPostfix.newTree(newIdentNode("*"), newIdentNode(
          "updateRow" & objName))
  let db = newIdentNode("db")
  let obj = newIdentNode("obj")
  let dbId = newIdentNode("dbId")
  let objType = newIdentNode(objName)

  proc genSerializedField(fieldName: string): NimNode =
    result = fmt"obj.{fieldName}.serialize()".parseStmt[0]
  let dbFields = fields.map(f => genSerializedField(f.fieldName))
  var dbCall = fmt"""db.exec(sql"{updateRowStr}", $times.now().toTime().toUnix())""".parseStmt[0]
  dbCall.add(dbFields)
  dbCall.add(dbId)

  result = quote do:
    proc `procName`(`db`: DbConn, `dbId`: int, `obj`: `objType`) =
      try:
        `dbCall`
      except:
        utils.error "Error updating row: " & getCurrentExceptionMsg()
        `db`.exec(sql"ROLLBACK")
        raise getCurrentException()

proc deleteRowProc*(objName: string): NimNode =
  let deleteRowStr = fmt"""DELETE FROM {objName}Table WHERE _dbID = ?"""
  let procName = nnkPostfix.newTree(newIdentNode("*"), newIdentNode(
          "deleteRow" & objName))
  let db = newIdentNode("db")
  let dbId = newIdentNode("dbId")
  let deleteRowSql = fmt"""sql("{deleteRowStr}")""".parseStmt[0]
  result = quote do:
    proc `procName`(`db`: DbConn, `dbId`: int) =
      try:
        `db`.exec(`deleteRowSql`, `dbId`)
      except:
        utils.error "Error deleting row: " & getCurrentExceptionMsg()
        `db`.exec(sql"ROLLBACK")
        raise getCurrentException()

proc dbRowToObj*[T](row: seq[string], skipEntries: int = 2): T =
  result = T.default
  var idx = -1
  for fieldName, fieldValue in result.fieldPairs:
    idx += 1
    if idx + skipEntries < row.len:
      # echo "Field: " & fieldName & ", Type: " & $fieldValue.typeof, ", Value: ",
      #   row[idx + skipEntries]
      fieldValue = row[idx + skipEntries].deserialize[:fieldValue.typeof]

proc queryRowsProc*(objName: string): NimNode =
  let queryRowsStr = fmt"""SELECT * FROM {objName}Table""".newStrLitNode
  let procName = nnkPostfix.newTree(newIdentNode("*"), newIdentNode(
          "queryRows" & objName))
  let db = newIdentNode("db")
  let objType = newIdentNode(objName)
  let whereClause = newIdentNode("whereClause")
  let row = newIdentNode("row")

  result = quote do:
    proc `procName`(`db`: DbConn, `whereClause`: string = ""): seq[tuple[
                dbId: int, data: `objType`]] =
      try:
        let rows = `db`.getAllRows(sql(`queryRowsStr` & (
                if `whereClause` != "": " WHERE " &
                `whereClause` else: "")))
        for `row` in rows:
          if `row`.len > 0:
            let dbId = `row`[0].parseInt
            let objData = `row`.dbRowToObj[:`objType`]()
            result.add((dbId, objData))
      except:
        utils.error "Error querying rows: " & getCurrentExceptionMsg()
        raise getCurrentException()

macro dbTable*(objDef: untyped): untyped =
  let (objName, fields) = getObjDefinitions(objDef)
  var objDefCopy = objDef.copyNimTree()

  # We need to ensure the type name has {.inject.} pragma
  # The name node is objDefCopy[0]

  var nameNode = objDefCopy[0]

  # Remove existing dbTable pragma if present in PragmaExpr to avoid recursion
  if nameNode.kind == nnkPragmaExpr:
    var newPragmas = nnkPragma.newTree()
    for p in nameNode[1]:
      if p.kind == nnkIdent and p.strVal == "dbTable":
        discard
      else:
        newPragmas.add(p)
    nameNode[1] = newPragmas

  # Now add inject
  if nameNode.kind == nnkPragmaExpr:
    nameNode[1].add("inject".newIdentNode)
  else:
    # Wrap in PragmaExpr
    # nameNode is Ident or Postfix
    let newPragmas = nnkPragma.newTree("inject".newIdentNode)
    nameNode = nnkPragmaExpr.newTree(nameNode, newPragmas)

  objDefCopy[0] = nameNode

  result = nnkTypeDef.newTree(
      newIdentNode(objName & "DbTable"),
      newEmptyNode(),
      nnkCall.newTree(
          newIdentNode("objDefPragmaHelper"),
          nnkStmtList.newTree(
              nnkTypeSection.newTree(
                  objDefCopy
    ),
    createTableProc(objName, fields),
    insertRowProc(objName, fields),
    insertRowsProc(objName, fields),
    updateRowProc(objName, fields),
    deleteRowProc(objName),
    queryRowsProc(objName),
    objName.newIdentNode
  )
    )
  )

#   echo result.repr
