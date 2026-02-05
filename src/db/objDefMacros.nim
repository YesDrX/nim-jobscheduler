import std/[macros, strformat]

export strformat

template objDefPragmaHelper*(code : untyped): untyped =
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

proc getObjDefinitions*(objDef : NimNode) : tuple[objName : string, fields : seq[tuple[fieldName, fieldType : string]]] =
    objDef.expectKind(nnkTypeDef)

    var objName : string    
    var nameNode = objDef[0]
    if nameNode.kind == nnkPragmaExpr:
        nameNode = nameNode[0]
        
    case nameNode.kind
    of nnkIdent:
        objName = nameNode.repr
    of nnkPostfix:
        objName = nameNode[^1].repr
    else:
        raise newException(ValueError, "Invalid object definition: " & $nameNode.kind)
    
    # get object fields
    var fieldsDef = objDef[2]
    if fieldsDef.kind == nnkRefTy:
        fieldsDef = fieldsDef[0]
    fieldsDef.expectKind(nnkObjectTy)
    let recList = fieldsDef[2]
    recList.expectKind(nnkRecList)
    var fields : seq[tuple[fieldName, fieldType : string]]
    for identDef in recList:
        var fieldName, fieldType : string
        
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
    result.fields  = fields
