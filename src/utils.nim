import std/[os, strutils, strformat, json, times]

proc jsonToYaml*(node: JsonNode, indentLevel: int = 0): string =
  ## Converts a JsonNode to YAML format recursively.
  let indentation = repeat("  ", indentLevel)

  case node.kind:
  of JObject:
    var lines: seq[string]
    if node.len == 0: return "{}" # Handle empty object

    for key, val in node:
      # If the value is a complex type (Object/Array), we need a new line
      if val.kind in {JObject, JArray}:
        lines.add(fmt"{indentation}{key}:")
        lines.add(jsonToYaml(val, indentLevel + 1))
      else:
        # Scalar values go on the same line
        lines.add(fmt"{indentation}{key}: {jsonToYaml(val, 0)}")
    return lines.join("\n")

  of JArray:
    var lines: seq[string]
    if node.len == 0: return "[]" # Handle empty array

    for item in node:
      # We render the item at the current indent + 2 spaces for the content
      let itemStr = jsonToYaml(item, indentLevel + 1).strip(leading = true)

      if item.kind == JObject:
        lines.add(fmt"{indentation}- {itemStr}")
      elif item.kind == JArray:
        lines.add(fmt"{indentation}-")
        lines.add(jsonToYaml(item, indentLevel + 1))
      else:
        lines.add(fmt"{indentation}- {itemStr}")
    return lines.join("\n")

  of JString:
    let s = node.getStr
    if "\n" in s:
      return "|\n" & s.indent(indentLevel + 2)
    return escape(s) # Basic escaping

  of JInt: return $node.getInt
  of JFloat: return $node.getFloat
  of JBool: return $node.getBool
  of JNull: return "null"

proc resolveCalendarPath*(calendarPath: string, yamlFilePath: string): string =
  ## Resolve calendar path relative to task YAML file if it's a relative path
  ## Cross-platform: handles Windows (C:, D:) and Unix (/) absolute paths
  if calendarPath == "":
    return ""

  # Check if path is absolute
  if isAbsolute(calendarPath):
    return calendarPath

  # Relative path: resolve relative to YAML file directory
  # strictly convert to absolute path so that checking code doesn't need to worry about CWD
  let absYamlPath = absolutePath(yamlFilePath)
  let yamlDir = parentDir(absYamlPath)
  let resolved = joinPath(yamlDir, calendarPath)

  return resolved

proc saveTaskToYaml*(data: JsonNode, path: string) =
  # Force enabled to true/false in JSON if missing, though usually present
  # Just dump what we have
  let yamlContent = jsonToYaml(data)
  writeFile(path, yamlContent)

proc getLogContent*(logPath: string, maxLines: int = 100): string =
  ## Reads the last maxLines lines of the log file
  if not fileExists(logPath): return ""

  # Cross-platform safe reading of last N lines
  try:
    let f = open(logPath, fmRead)
    defer: f.close()

    # Check size. If small, read all.
    let size = f.getFileSize()
    if size < 5000: # 5KB, small enough
      let content = f.readAll()
      let lines = content.splitLines()
      if lines.len <= maxLines:
        return content
      else:
        return lines[^maxLines..^1].join("\n")

    # If large, perform seek-based reading (simplified for now: read last 10KB and split)
    let readSize = min(size, 20_000) # Read last 20KB
    f.setFilePos(-readSize, fspEnd)
    let content = f.readAll()
    let lines = content.splitLines()

    # The first line might be partial, discard it if we read from middle
    var startIdx = 0
    if readSize < size:
      startIdx = 1

    if lines.len - startIdx <= maxLines:
      return lines[startIdx..^1].join("\n")
    else:
      return lines[^maxLines..^1].join("\n")

  except:
    return "Error reading log file"

type
  LogLevel* = enum
    DEBUG
    INFO
    WARN
    ERROR
    FATAL

var
  loggerLevel = LogLevel.INFO

proc setLogLevel*(level: LogLevel) =
  {.gcsafe.}:
    loggerLevel = level

proc log*(lvl: LogLevel, msg: string) =
  {.gcsafe.}:
    if lvl >= loggerLevel:
      echo "[", $lvl, "]", "[", now().format("yyyyMMdd HH:mm:ss"), "] ", msg
    if lvl == FATAL:
      raise newException(Exception, msg)

proc debug*(msg: string) = log(DEBUG, msg)
proc info*(msg: string) = log(INFO, msg)
proc warn*(msg: string) = log(WARN, msg)
proc error*(msg: string) = log(ERROR, msg)
proc fatal*(msg: string) = log(FATAL, msg)

proc sanitizeFileName*(name: string): string =
  result = name.replace(' ', '_')
  for ch in {',', ':', ';', '*', '[', ']', '(', ')', '{', '}', '|', '?', '"',
      '<', '>'}:
    result = result.replace(ch, '_')

proc readFile*(filePath: string, maxLen: int): tuple[skippedSomeBytes: bool,
    content: string] =
  if not fileExists(filePath): return (false, "")
  var f: File
  if not open(f, filePath):
    raise newException(IOError, "Could not open file: " & filePath)
  defer: close(f)
  let fileSize = f.getFileSize()
  let skipBytes = max(0, fileSize - maxLen)
  if skipBytes >= fileSize: return (false, "")
  f.setFilePos(skipBytes)
  return (skipBytes > 0, if skipBytes > 0: "...\n" & f.readAll() else: f.readAll())
