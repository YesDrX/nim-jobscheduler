# Package

version       = "0.6.0"
author        = "User"
description   = "A full-featured job scheduler in Nim"
license       = "MIT"
srcDir        = "src"
bin           = @["jobscheduler"]


# Dependencies

requires "nim >= 2.0.0"
requires "yaml"
requires "db_connector"
requires "checksums"
requires "smtp"
requires "timezones"
requires "nimja"
requires "cligen"
