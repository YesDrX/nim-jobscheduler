
import std/[unittest, times, options, strutils, os]
import ../src/models/types
import ../src/models/task
import ../src/scheduler/triggers

suite "On Start Logic":
  
  test "On Start Trigger - First Run":
    let now = parse("2026-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss", utc())
    let serverStart = parse("2026-01-01 11:59:00", "yyyy-MM-dd HH:mm:ss", utc())
    
    var task = Task(
      name: "Test OnStart",
      scheduleType: stOnStart,
      timezone: "UTC",
      enabled: true,
      createdAt: now
    )
    
    # Last Run is NONE -> Should Trigger
    let nextRun = getNextTrigger(task, now, none(DateTime), some(serverStart))
    check nextRun.isSome
    check nextRun.get() == now # Should return 'now' (or close to it)

    # Check boolean trigger
    check shouldTrigger(task, now, none(DateTime), serverStart) == true

  test "On Start Trigger - Already Run This Session":
    let now = parse("2026-01-01 12:05:00", "yyyy-MM-dd HH:mm:ss", utc())
    let serverStart = parse("2026-01-01 11:59:00", "yyyy-MM-dd HH:mm:ss", utc())
    let lastRun = parse("2026-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss", utc()) # Ran after start
    
    var task = Task(
      name: "Test OnStart",
      scheduleType: stOnStart,
      timezone: "UTC",
      enabled: true,
      createdAt: now
    )
    
    # Last Run > ServerStart -> Should NOT Trigger
    let nextRun = getNextTrigger(task, now, some(lastRun), some(serverStart))
    check nextRun.isNone
    
    check shouldTrigger(task, now, some(lastRun), serverStart) == false

  test "On Start Trigger - Restarted (Last Run before Start)":
    let now = parse("2026-01-01 13:00:00", "yyyy-MM-dd HH:mm:ss", utc())
    let serverStart = parse("2026-01-01 12:59:00", "yyyy-MM-dd HH:mm:ss", utc()) # Restarted recently
    let lastRun = parse("2026-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss", utc()) # Ran before current start
    
    var task = Task(
      name: "Test OnStart",
      scheduleType: stOnStart,
      timezone: "UTC",
      enabled: true,
      createdAt: now
    )
    
    # Last Run < ServerStart -> Should Trigger
    let nextRun = getNextTrigger(task, now, some(lastRun), some(serverStart))
    check nextRun.isSome
    check nextRun.get() == now

    check shouldTrigger(task, now, some(lastRun), serverStart) == true
