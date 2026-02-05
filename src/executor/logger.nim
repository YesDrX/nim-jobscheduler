import std/[os, times, strutils]

proc ensureLogDir*(path: string) =
  createDir(path)

proc generateLogHeader*(taskName, jobName: string, isRemote: bool,
    host: string = "", user: string = "", command : string = ""): string =
  var s = "=== Execution Metadata ===\n"
  s.add "Task: " & taskName & "\n"
  s.add "Job: " & jobName & "\n"
  if isRemote:
    s.add "Type: Remote (SSH)\n"
    s.add "Host: " & host & "\n"
    s.add "User: " & user & "\n"
  else:
    s.add "Type: Local\n"
  s.add "Start Time: " & $now() & "\n"
  s.add "Command: " & command & "\n"
  s.add "==========================\n\n"
  return s

proc rotateLogs*(logsDir: string, maxAgeDays: int) =
  ## Deletes logs older than maxAgeDays
  if not dirExists(logsDir): return

  let cutoff = now() - maxAgeDays.days
  for kind, path in walkDir(logsDir, relative = false):
    if kind == pcFile:
      # Check modification time
      if getLastModificationTime(path).utc < cutoff.utc:
        try:
          removeFile(path)
        except:
          discard

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
