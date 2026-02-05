import std/[unittest, times, options, strutils]
import ../src/scheduler/triggers
import ../src/models/task
import ../src/models/types

suite "DateList Triggers":
  
  setup:
    let now = now()
    let todayYMD = now.format("yyyy-MM-dd")
    
  test "DateList Check Interval":
    var task = Task(
      scheduleType: stDateList,
      dateList: @[todayYMD],
      intervalMinutes: 10,
      enabled: true
    )
    
    # 1. First run (no last run) -> Should trigger
    check shouldTrigger(task, now, none(DateTime)) == true
    
    # 2. Run recently (within interval) -> Should NOT trigger
    let lastRunRecent = some(now - 5.minutes)
    check shouldTrigger(task, now, lastRunRecent) == false
    
    # 3. Run long ago (past interval) -> Should trigger
    let lastRunOld = some(now - 15.minutes)
    check shouldTrigger(task, now, lastRunOld) == true
    
  test "DateList Invalid Date":
    let yesterdayYMD = (now - 1.days).format("yyyy-MM-dd")
    var task = Task(
      scheduleType: stDateList,
      dateList: @[yesterdayYMD], # Only valid yesterday
      intervalMinutes: 10,
      enabled: true
    )
    
    # Even if never run, should NOT trigger because date doesn't match
    check shouldTrigger(task, now, none(DateTime)) == false
    
  test "DateList Check TimeOfDay":
    var task = Task(
      scheduleType: stDateList,
      dateList: @[todayYMD],
      timeOfDay: now.format("HH:mm"),
      enabled: true
    )
    
    # Should trigger at matching time
    check shouldTrigger(task, now, none(DateTime)) == true
    
    # Should NOT trigger if already run today
    let earlierToday = some(now - 10.minutes)
    check shouldTrigger(task, now, earlierToday) == false
