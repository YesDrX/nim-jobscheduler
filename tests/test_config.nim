import unittest, os
import config/loader
import config/types

suite "Config Loader Tests":
  test "Load Example Config":
    let path = "config.example.yaml"
    check fileExists(path)
    
    let cfg = loadConfig(path)
    
    check cfg.tasksDir == "tasks"
    check cfg.database.path == "jobscheduler.db"
    check cfg.server.port == 8080
    check cfg.smtp.enabled == false
    check cfg.internal.logRetentionDays == 30
