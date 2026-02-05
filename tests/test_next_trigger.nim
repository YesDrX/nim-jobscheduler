import std/[unittest, times, options, strutils, os]
import ../src/models/types
import ../src/models/task
import ../src/scheduler/triggers

suite "Next Trigger Logic":
  
  test "Cron Next Trigger":
    let now = parse("2025-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss", utc())
    var task = Task(
      name: "Test Cron",
      scheduleType: stCron,
      cronExpr: "0 13 * * *", # Every day at 13:00
      timezone: "UTC",
      enabled: true,
      createdAt: now
    )
    
    let nextRun = getNextTrigger(task, now, none(DateTime))
    check nextRun.isSome
    check nextRun.get() == parse("2025-01-01 13:00:00", "yyyy-MM-dd HH:mm:ss", utc())

  test "Time Logic Next Trigger - Same Day":
    let now = parse("2025-01-01 10:00:00", "yyyy-MM-dd HH:mm:ss", utc())
    var task = Task(
      name: "Test Time",
      scheduleType: stTime,
      timeOfDay: "15:30",
      timezone: "UTC",
      enabled: true,
      createdAt: now
    )
    
    let nextRun = getNextTrigger(task, now, none(DateTime))
    check nextRun.isSome
    check nextRun.get() == parse("2025-01-01 15:30:00", "yyyy-MM-dd HH:mm:ss", utc())

  test "Time Logic Next Trigger - Next Day":
    let now = parse("2025-01-01 16:00:00", "yyyy-MM-dd HH:mm:ss", utc())
    var task = Task(
      name: "Test Time",
      scheduleType: stTime,
      timeOfDay: "15:30",
      timezone: "UTC",
      enabled: true,
      createdAt: now
    )
    
    let nextRun = getNextTrigger(task, now, none(DateTime))
    check nextRun.isSome
    check nextRun.get() == parse("2025-01-02 15:30:00", "yyyy-MM-dd HH:mm:ss", utc())

  test "Interval Next Trigger":
    let now = parse("2025-01-01 10:00:00", "yyyy-MM-dd HH:mm:ss", utc())
    let lastRun = parse("2025-01-01 09:50:00", "yyyy-MM-dd HH:mm:ss", utc())
    var task = Task(
      name: "Test Interval",
      scheduleType: stInterval,
      intervalMinutes: 30,
      timezone: "UTC",
      enabled: true,
      createdAt: now
    )
    
    # Last run 9:50, interval 30m -> next 10:20
    let nextRun = getNextTrigger(task, now, some(lastRun))
    check nextRun.isSome
    check nextRun.get() == parse("2025-01-01 10:20:00", "yyyy-MM-dd HH:mm:ss", utc())

  test "Interval Immediate (No Last Run)":
    let now = parse("2025-01-01 10:00:00", "yyyy-MM-dd HH:mm:ss", utc())
    var task = Task(
      name: "Test Interval",
      scheduleType: stInterval,
      intervalMinutes: 30,
      timezone: "UTC",
      enabled: true,
      createdAt: now
    )
    
    # No last run -> Run Immediately (now)
    let nextRun = getNextTrigger(task, now, none(DateTime))
    check nextRun.isSome
    # It should be roughly 'now' or maybe slightly in future? 
    # Current logic usually interprets interval without last run as "Right Now", 
    # but `getNextTrigger` implies "Future". 
    # If it's runnable now, it should return now.
    check nextRun.get() <= now + 1.seconds # Close enough

  test "Calendar File Restriction":
    let now = parse("2025-01-01 10:00:00", "yyyy-MM-dd HH:mm:ss", utc())
    
    # Mock dateList (simulating loaded calendar)
    let dateList = @["2025-01-02", "2025-01-05"]
    
    var task = Task(
      name: "Test Calendar",
      scheduleType: stTime,
      timeOfDay: "12:00",
      timezone: "UTC",
      enabled: true,
      calendarPath: "test_calendar.txt", # Path still needed for logic triggering check
      dateList: dateList,
      createdAt: now
    )
    
    # Next trigger should be on 2025-01-02 12:00 (skip 2025-01-01)
    let nextRun = getNextTrigger(task, now, none(DateTime))
    check nextRun.isSome
    check nextRun.get() == parse("2025-01-02 12:00:00", "yyyy-MM-dd HH:mm:ss", utc())
    
    # Test skipping invalid days
    let now2 = parse("2025-01-03 10:00:00", "yyyy-MM-dd HH:mm:ss", utc())
    let nextRun2 = getNextTrigger(task, now2, none(DateTime))
    # Should skip 3rd and 4th, land on 5th
    check nextRun2.isSome
    check nextRun2.get() == parse("2025-01-05 12:00:00", "yyyy-MM-dd HH:mm:ss", utc())

