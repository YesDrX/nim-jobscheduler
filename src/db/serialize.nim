import strutils, json, times, db_connector/db_sqlite

# Serialization helpers
proc serialize*[T](val: T): string =
  when T is string:
    return val
  elif T is int or T is int64:
    return $val
  elif T is float or T is float64:
    return $val
  elif T is bool:
    return if val: "1" else: "0"
  elif T is DateTime:
    return val.format("yyyy-MM-dd HH:mm:ss")
  elif T is enum:
    return $val
  else:
    return $(%val) # Fallback to JSON for complex types

# Deserialization helpers
proc deserialize*[T](val: string): T =
  when T is string:
    return val
  elif T is int:
    return parseInt(val)
  elif T is int64:
    return parseBiggestInt(val)
  elif T is float:
    return parseFloat(val)
  elif T is bool:
    return val == "1"
  elif T is DateTime:
    return parse(val, "yyyy-MM-dd HH:mm:ss", utc())
  elif T is enum:
    return parseEnum[T](val)
  else:
    # Fallback to JSON
    return to(parseJson(val), T)
