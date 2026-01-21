import unittest, times, options
import scheduler/triggers
import models/task
import models/types
import timezones

suite "Timezone Trigger Tests":
  
  test "Timezone-aware TimeOfDay Trigger":
    # Australia/Sydney is UTC+11 in January (DST) or UTC+10 (Standard).
    # Let's pick a date in January (DST). 2024-01-01. Offset +11.
    # 09:00 Sydney = 22:00 UTC (previous day)
    
    var task = Task(
      name: "SydneyTask",
      enabled: true,
      scheduleType: stTime,
      timeOfDay: "09:00",
      timezone: "Australia/Sydney",
      intervalMinutes: 60
    )
    
    # Case 1: Time matches in Sydney
    # 2024-01-01T22:00:00Z -> 2024-01-02T09:00:00+11:00
    # Use initDateTime to avoid local timezone ambiguity
    let matchTime = initDateTime(1, mJan, 2024, 22, 0, 0, utc())
    check shouldTrigger(task, matchTime, none(DateTime)) == true
    
    # Case 2: Time matches in UTC but not Sydney
    # 2024-01-01T09:00:00Z 
    let nonMatchTime = initDateTime(1, mJan, 2024, 9, 0, 0, utc())
    check shouldTrigger(task, nonMatchTime, none(DateTime)) == false
    
  test "Timezone-aware Cron Trigger":
    # 09:00 Sydney = 22:00 UTC previous day
    var task = Task(
      name: "SydneyCron",
      enabled: true,
      scheduleType: stCron,
      cronExpr: "* 9 * * *", # Run at 9am every day
      timezone: "Australia/Sydney",
      createdAt:       initDateTime(31, mDec, 2023, 0, 0, 0, utc())
    )
    
    # If now is 2023-12-31 22:01 UTC.
    let nowTime = initDateTime(31, mDec, 2023, 22, 1, 0, utc())
    check shouldTrigger(task, nowTime, none(DateTime)) == true
    
    # If now is earlier 2023-12-31 21:00 UTC
    let earlyTime = initDateTime(31, mDec, 2023, 21, 0, 0, utc())
    check shouldTrigger(task, earlyTime, none(DateTime)) == false
