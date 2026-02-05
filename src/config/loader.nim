import yaml, streams, os, logging
import ./types

proc loadConfig*(path: string): Config =
  if not fileExists(path):
    raise newException(IOError, "Config file not found: " & path)
  
  var s = newFileStream(path)
  defer: s.close()
  load(s, result)

  info "Loaded config:\n" & $result

proc saveConfig*(path: string, cfg: Config) =
  var s = newFileStream(path, fmWrite)
  defer: s.close()
  var dumper = Dumper()
  dumper.dump(cfg, s)
