import std/[json, tables, times, strutils, options]
export json
import ./utils
export utils

proc `%`*(val: DateTime): JsonNode =
    if not val.isInitialized:
        return newJNull()
    return val.format("yyyy-MM-dd HH:mm:ss zzzz").`%`

proc deserializeJson*[T](val: JsonNode): T

proc deserializeJsonSeq*[T](val: JsonNode, rst: var seq[T]) =
    if val.kind == JNull:
        return
    for item in val.items:
        rst.add(deserializeJson[T](item))

proc deserializeJsonTable*[K, V](val: JsonNode, rst: var Table[K, V]) =
    if val.kind == JNull:
        return
    for key, value in val.pairs:
        rst[deserializeJson[K](key)] = deserializeJson[V](value)

proc deserializeJson*[T](val: JsonNode): T =
    when T is string:
        return val.getStr()
    elif T is SomeInteger:
        return val.getInt().T
    elif T is SomeFloat:
        return val.getFloat().T
    elif T is bool:
        return val.getBool().T
    elif T is enum:
        return parseEnum[T](val.getStr())
    elif T is Option:
        type InnerT = typeof(default(T).get())
        if val.kind == JNull or (val.kind == JString and val.getStr() == ""):
            return none(InnerT)
        else:
            return some(deserializeJson[InnerT](val))
    elif T is DateTime:
        if val.kind == JNull or val.getStr().len == 0:
            return default(DateTime)
        return parse(val.getStr(), "yyyy-MM-dd HH:mm:ss zzzz")
    elif T is seq:
        deserializeJsonSeq(val, result)
    elif T is Table:
        deserializeJsonTable(val, result)
    elif T is ref object:
        result = new(T)
        for key, value in result[].fieldPairs:
            if key in val:
                value = deserializeJson[type(value)](val[key])
        return result
    elif T is object:
        result = default(T)
        for key, value in result.fieldPairs:
            if key in val:
                value = deserializeJson[type(value)](val[key])
    else:
        raise newException(ValueError, "Deserializing " & $T & " not implemented")

proc deserializeJson*[T](val: JsonNode, rst: var T) =
    when T is object:
        for key, value in rst.fieldPairs:
            if key in val:
                value = deserializeJson[type(value)](val[key])
    elif T is ref object:
        if rst.isNil:
            rst = new(T)
        for key, value in rst[].fieldPairs:
            if key in val:
                value = deserializeJson[type(value)](val[key])
    else:
        rst = deserializeJson[T](val)

#forward declare
proc serialize*[T](val: T): string
proc deserialize*[T](val: string): T

proc serializeSeq*[T](val: seq[T]): string =
    result = "["
    for i, item in val.pairs:
        result &= serialize(item)
        if i < val.len - 1:
            result &= "||,||"
    result &= "]"

proc serializeTable*[K, V](val: Table[K, V]): string =
    if val.len == 0:
        return "{}"

    result = "{"
    for (key, value) in val.pairs:
        result &= serialize(key) & ':' & serialize(value) & "!|,|!"
    result = result[0..^2] & "}"

proc deserializeSeq*[T](val: string, rst: var seq[T]) =
    if val.len == 0:
        return
    if val == "null":
        return

    if val[0] != '[' or val[^1] != ']':
        raise newException(ValueError, "Invalid sequence")

    let items = val[1..^2].split("||,||")
    for item in items:
        rst.add(deserialize[T](item))

proc deserializeTable*[K, V](val: string, rst: var Table[K, V]) =
    if val.len == 0:
        return
    if val == "null":
        return

    if val[0] != '{' or val[^1] != '}':
        raise newException(ValueError, "Invalid table")

    let items = val[1..^2].split("!|,|!")
    for item in items:
        let key_value = item.split(":")
        if key_value.len != 2:
            raise newException(ValueError, "Invalid table")
        let key = key_value[0]
        let value = key_value[1]
        rst[deserialize[K](key)] = deserialize[V](value)

proc serialize*[T](val: T): string =
    return $(%*val)
    # when T is string:
    #     return '"' & val & '"'
    # elif T is SomeInteger:
    #     return $val
    # elif T is SomeFloat:
    #     return $val
    # elif T is bool:
    #     return $val
    # elif T is Option:
    #     if val.isSome:
    #         return serialize(val.get)
    #     else:
    #         return "null"
    # elif T is enum:
    #     return '"' & $val & '"'
    # elif T is seq:
    #     return serializeSeq(val)
    # elif T is Table:
    #     return serializeTable(val)
    # elif T is DateTime:
    #     if not val.isInitialized:
    #         return ""
    #     return '"' & val.format("yyyy-MM-dd HH:mm:ss zzzz") & '"'
    # elif T is ref object:
    #     result = "{"
    #     for key, value in val[].fieldPairs:
    #         result &= serialize(key) & ":" & serialize(value) & ","
    #     result = result[0..^2] & "}"
    # elif T is object:
    #     result = "{"
    #     for key, value in val.fieldPairs:
    #         result &= serialize(key) & ":" & serialize(value) & ","
    #     result = result[0..^2] & "}"
    # else:
    #     raise newException(ValueError, "Serializing " & $T & " not implemented")

proc deserialize*[T](val: string): T =
    # echo "Deserializing " & $T & " from " & val
    return deserializeJson[T](parseJson(val))

    # when T is string:
    #     return val.strip(chars = {'"'})
    # elif T is SomeInteger:
    #     return parseInt(val).T
    # elif T is SomeFloat:
    #     return parseFloat(val).T
    # elif T is bool:
    #     return parseBool(val).T
    # elif T is enum:
    #     return parseEnum[T](val.strip(chars = {'"'}))
    # elif T is Option:
    #     if val == "null" or val == "":
    #         return none(typeof(default(T).get()))
    #     else:
    #         return some(deserialize[typeof(default(T).get())](val))
    # elif T is DateTime:
    #     if val.len == 0 or val == "null":
    #         return default(DateTime)
    #     return parse(val.strip(chars = {'"'}), "yyyy-MM-dd HH:mm:ss zzzz")
    # elif T is seq:
    #     var rst: T
    #     deserializeSeq(val, rst)
    #     return rst
    # elif T is Table:
    #     var rst: T
    #     deserializeTable(val, rst)
    #     return rst
    # elif T is ref object:
    #     result = new(T)
    #     let jsonVal = parseJson(val)
    #     for key, value in result[].fieldPairs:
    #         if key in jsonVal:
    #             value = deserialize[type(value)]($jsonVal[key])
    #     return result
    # elif T is object:
    #     let jsonVal = parseJson(val)
    #     for key, value in result.fieldPairs:
    #         if key in jsonVal:
    #             value = deserialize[type(value)]($jsonVal[key])
    # else:
    #     raise newException(ValueError, "Deserializing " & $T & " not implemented")

proc getIndention(indent: int): string =
    return "  ".repeat(indent)

proc toYamlString*[T](val: T, indent: int = 0): string =
    when T is string:
        return val
    elif T is SomeInteger or T is SomeFloat or T is bool:
        return $val
    elif T is enum:
        return $val
    elif T is Option:
        if val.isSome:
            return toYamlString(val.get, indent)
        else:
            return "null"
    elif T is DateTime:
        if not val.isInitialized:
            return "null"
        return val.format("yyyy-MM-dd HH:mm:ss zzzz")
    elif T is seq:
        result &= "\n"
        for idx, item in val.pairs:
            result &= getIndention(indent) & "- " & toYamlString(item, indent +
                    1) & "\n"
    elif T is Table:
        result &= "\n"
        for key, value in val.pairs:
            result &= getIndention(indent) & $key & ": " & toYamlString(
                    value, indent + 1) & "\n"
    elif T is object:
        result &= "\n"
        for fld, value in val.fieldPairs:
            if (typeof(value) is not string) or (value != default(typeof(value))):
                result &= getIndention(indent) & $fld & ": " & toYamlString(
                        value, indent + 1) & "\n"
    elif T is ref object:
        result &= "\n"
        for fld, value in val[].fieldPairs:
            if (typeof(value) is not string) or (value != default(typeof(value))):
                result &= getIndention(indent) & $fld & ": " & toYamlString(
                        value, indent + 1) & "\n"
    else:
        raise newException(ValueError, "Serializing " & $T & " not implemented")


when isMainModule:
    type
        TestEnum = enum
            A = 1
            B = 2

        ChildTestObj = object
            childName: string
            childAge: int32
            childSeq: seq[int]

        TestObj = object
            name: string
            age: int32
            weight: float
            isHappy: bool
            child: ChildTestObj
            enumVal: TestEnum
            createdAt: DateTime
            tags: seq[string]
            metadata: Table[string, string]

        TestObjRef = ref TestObj


    let obj = TestObjRef(
        name: "Test",
        age: 25,
        child: ChildTestObj(
            childName: "Child",
            childAge: 10,
            childSeq: @[1, 2, 3]
        ),
        enumVal: A,
        createdAt: now(),
        tags: @["tag1", "tag2"],
        metadata: {"key1": "value1", "key2": "value2"}.toTable
    )

    # let serialized = serialize(obj)
    # echo serialized

    # let deserialized = deserialize[TestObjRef](serialized)
    # echo deserialized[]

    echo toYamlString(obj)

