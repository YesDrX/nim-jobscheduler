import unittest, os, strutils, options
import services/task_file_service
import models/types
import models/task

const TestPartialDir = "test_partial_tasks"

suite "Partial YAML Tests":
  setup:
    if dirExists(TestPartialDir): removeDir(TestPartialDir)
    createDir(TestPartialDir)

  teardown:
    if dirExists(TestPartialDir): removeDir(TestPartialDir)

  test "Load partial YAML with missing optional fields":
    let yamlContent = """
task:
  name: PartialTask
  taskType: local
  scheduleType: cron
  # Missing description, ssh fields, etc.
  enabled: true
jobs:
  - command: "echo 1"
    enabled: true
"""
    let filename = TestPartialDir / "PartialTask.yaml"
    writeFile(filename, yamlContent)
    
    let taskFileOpt = loadTaskFromYaml(filename)
    check taskFileOpt.isSome
    let tf = taskFileOpt.get
    
    check tf.task.name == "PartialTask"
    check tf.task.description == "" # Default
    check tf.task.taskType == ttLocal
    check tf.jobs.len == 1
    check tf.jobs[0].command == "echo 1"
    
  test "Load partial YAML with missing scheduleType (defaulting)":
    let yamlContent = """
task:
  name: DefaultTask
  # Missing taskType (default local?), scheduleType (default cron?)
  enabled: true
jobs: []
"""
    let filename = TestPartialDir / "DefaultTask.yaml"
    writeFile(filename, yamlContent)
    
    let taskFileOpt = loadTaskFromYaml(filename)
    check taskFileOpt.isSome
    let tf = taskFileOpt.get
    
    check tf.task.name == "DefaultTask"
    check tf.task.taskType == ttLocal # Default
    check tf.task.scheduleType == stCron # Default
