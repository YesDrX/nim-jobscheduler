import std/[times, options, strutils, strscans, os]
import timezones
import ../models/types
import ../models/task
import ../config/task_loader
import cron

# Helper to get timezone from string, defaulting to UTC
# Helper to get timezone from string, defaulting to UTC
proc parseTimezone(tzName: string): Timezone =
  if tzName == "UTC": return utc()
  if tzName == "" or tzName == "Local": return local()

  try:
    return timezones.tz(tzName)
  except:
    echo "DEBUG TIMEZONE ERROR: " & tzName & " : " & getCurrentExceptionMsg()
    # If invalid or unknown, fallback to UTC and maybe log (but can't log easily here without import logging)
    return utc()

proc checkCron(task: Task, now: DateTime, lastRun: Option[DateTime]): bool =
  if task.cronExpr == "": return false

  let cron = parseCron(task.cronExpr)
  if cron.isNone: return false # Invalid cron
  
  # Reference time is lastRun or createdAt
  # We work in the Task's timezone
  let tz = parseTimezone(task.timezone)
  let nowInTz = now.inZone(tz)

  # Determine the reference time to calculate "Next Run" from
  var referenceTime: DateTime

  if lastRun.isSome:
    referenceTime = lastRun.get().inZone(tz)
  else:
    # If never run, check from creation time
    referenceTime = task.createdAt.inZone(tz)
    # Catchup logic: If created long ago, we might trigger immediately.

  # Use cron nextRun logic
  try:
    let nextTime = cron.get().nextRun(referenceTime)

    # If next scheduled time has passed (or is now)
    if nextTime <= nowInTz:
      return true

  except ValueError:
    return false # No next time found

  return false

proc checkTimeOfDay(task: Task, now: DateTime, lastRun: Option[
    DateTime]): bool =
  if task.timeOfDay == "": return false

  # Format expected: HH:MM
  var h, m: int
  if not scanf(task.timeOfDay, "$i:$i", h, m): return false

  let tz = parseTimezone(task.timezone)
  let nowInTz = now.inZone(tz)

  # Check if matches current time (within the minute minute)
  if nowInTz.hour == h and nowInTz.minute == m:
    # Deduplicate: Don't run again if lastRun was today
    if lastRun.isSome:
      let lastInTz = lastRun.get().inZone(tz)
      if lastInTz.year == nowInTz.year and lastInTz.month == nowInTz.month and
          lastInTz.monthday == nowInTz.monthday:
        return false
    return true

  return false

proc checkInterval(task: Task, now: DateTime, lastRun: Option[DateTime]): bool =
  if task.intervalMinutes <= 0: return false

  let tz = parseTimezone(task.timezone)
  let nowInTz = now.inZone(tz)

  # Parse window if present (HH:MM-HH:MM)
  var startH, startM, endH, endM: int
  var hasWindow = false
  if task.intervalStart != "" and task.intervalEnd != "":
    if scanf(task.intervalStart, "$i:$i", startH, startM) and scanf(
        task.intervalEnd, "$i:$i", endH, endM):
      hasWindow = true

  # Check Window
  if hasWindow:
    let curMins = nowInTz.hour * 60 + nowInTz.minute
    let startMins = startH * 60 + startM
    let endMins = endH * 60 + endM

    if startMins <= endMins:
      # Simple range (e.g. 09:00 - 17:00)
      if curMins < startMins or curMins > endMins: return false
    else:
      # Cross midnight range (e.g. 23:00 - 02:00)
      if curMins < startMins and curMins > endMins: return false

  # Check Time passed
  if lastRun.isNone:
    return true # Run immediately if inside window

  let last = lastRun.get()
  # Use UTC for duration diff to be safe
  let diff = now - last
  if diff.inMinutes >= task.intervalMinutes:
    return true

  return false



proc checkCalendar(task: Task, now: DateTime): bool =
  if task.calendarPath == "": return true # No calendar = allow all
  
  # If calendar path is set but no dates loaded, maybe warn?
  # For now, if dateList is empty, we assume allow all OR block all?
  # usually dateList being empty with a path means "not loaded" or "empty file".
  # If file exists but empty, block all?
  # To match previous behavior: lines(xxx) would give nothing, loop finishes, return false.

  if task.dateList.len == 0:
    return false

  try:
    let tz = parseTimezone(task.timezone)
    let inZone = now.inZone(tz)
    let todayYMD = inZone.format("yyyy-MM-dd")
    let todayCompact = inZone.format("yyyyMMdd")

    for line in task.dateList:
      let cleanLine = line.strip()
      if cleanLine == "" or cleanLine.startsWith("#"): continue
      if cleanLine == todayYMD or cleanLine == todayCompact:
        return true
  except:
    return false

  return false

proc shouldTrigger*(task: Task, now: DateTime, lastRun: Option[DateTime],
    serverStartTime: DateTime): bool =
  ## Main entry point to check if a task should trigger
  if not task.enabled: return false

  # Check Calendar Constraint first (only for Time and Interval)
  if task.scheduleType in {stTime, stInterval}:
    if not checkCalendar(task, now): return false

  case task.scheduleType:
  of stCron: return checkCron(task, now, lastRun)
  of stTime: return checkTimeOfDay(task, now, lastRun)
  of stInterval: return checkInterval(task, now, lastRun)
  of stOnStart:
    # Trigger if not run yet (lastRun is None)
    if lastRun.isNone: return true
    # Or if lastRun was BEFORE the current server start time (meaning we restarted)
    if lastRun.get() < serverStartTime: return true
    return false

proc getTaskWarnings*(task: Task): seq[string] =
  result = @[]

  # 1. Check Schedule Configuration
  case task.scheduleType:
  of stCron:
    if task.cronExpr == "":
      result.add("Cron expression is missing")
    elif parseCron(task.cronExpr).isNone:
      result.add("Invalid cron expression: " & task.cronExpr)
  of stTime:
    if task.timeOfDay == "":
      result.add("Time of day is missing")
    else:
      var h, m: int
      if not scanf(task.timeOfDay, "$i:$i", h, m):
        result.add("Invalid time format (use HH:MM): " & task.timeOfDay)
  of stInterval:
    if task.intervalMinutes <= 0:
      result.add("Interval must be greater than 0 minutes")
  of stOnStart:
    discard

  # 2. Check Timezone
  if task.timezone != "" and task.timezone != "Local" and task.timezone != "UTC":
    try:
      discard timezones.tz(task.timezone)
    except:
      result.add("Invalid timezone: " & task.timezone)

  # 3. Check Calendar File
  if task.calendarPath != "" and task.scheduleType in {stTime, stInterval}:
    let fullPath = resolveCalendarPath(task.calendarPath, task.sourceFile)
    let exists = fileExists(fullPath)
    if not exists:
      result.add("Calendar file not found: " & task.calendarPath)

proc isDateAllowed*(dateList: seq[string], date: DateTime): bool =
  if dateList.len == 0: return true # Wait, if list empty but path set? 
  # Actually, `isDateAllowed` is called inside loop.
  # If we rely on task having dateList, we should check task.calendarPath existence outside or pass it context.
  # But simpler: if dateList has items, check them. If not...
  # If task has calendarPath but empty dateList -> Block (same as empty file)
  # But here we only pass dateList. 
  # Caller handles "no calendar path" -> pass nothing or check before calling.

  try:
    let dateYMD = date.format("yyyy-MM-dd")
    let dateCompact = date.format("yyyyMMdd")
    for line in dateList:
      let clean = line.strip()
      if clean == "" or clean.startsWith("#"): continue
      if clean == dateYMD or clean == dateCompact:
        return true
  except:
    return false
  return false

proc getNextTrigger*(task: Task, now: DateTime, lastRun: Option[DateTime],
    serverStartTime: Option[DateTime] = none(DateTime)): Option[DateTime] =
  if not task.enabled: return none(DateTime)

  let tz = parseTimezone(task.timezone)
  let nowInTz = now.inZone(tz)

  # Max search window (e.g. 365 days)
  let MaxSearchDays = 365

  var candidate: Option[DateTime] = none(DateTime)

  case task.scheduleType:
  of stCron:
    if task.cronExpr == "": return none(DateTime)
    let cronOpt = parseCron(task.cronExpr)
    if cronOpt.isNone: return none(DateTime)

    # Try next occurrences (Standard Cron, ignores calendar)
    try:
      let next = cronOpt.get().nextRun(nowInTz)
      return some(next)
    except:
      return none(DateTime)

  of stTime:
    if task.timeOfDay == "": return none(DateTime)
    var h, m: int
    if not scanf(task.timeOfDay, "$i:$i", h, m): return none(DateTime)

    # Check today, then tomorrow, etc.
    var currentDay = nowInTz
    # Reset to simple date at 00:00 for loop iteration (cleaner?)
    # Or just construct candidates

    # First candidate: Today at H:M
    var checkTime = dateTime(currentDay.year, currentDay.month,
        currentDay.monthday, h, m, 0, 0, tz)
    if checkTime <= nowInTz:
      checkTime += 1.days


    for i in 0..MaxSearchDays:
      # If calendarPath is set, we must check against dateList.
      # If calendarPath is empty, always allowed.
      if task.calendarPath != "" and not isDateAllowed(task.dateList, checkTime):
        checkTime += 1.days
        continue

      return some(checkTime)
      checkTime += 1.days

  of stInterval:
    if task.intervalMinutes <= 0: return none(DateTime)

    # Grid Alignment Logic
    var startH, startM, endH, endM: int
    let hasWindow = (task.intervalStart != "" and task.intervalEnd != "") and
                    scanf(task.intervalStart, "$i:$i", startH, startM) and
                    scanf(task.intervalEnd, "$i:$i", endH, endM)

    # Determine reference time
    # User issue: If lastRun is old, we generate thousands of triggers.
    # Fix: Always start searching from NOW (or lastRun if it's somehow in future)
    # This effectively "Skips Missed" intervals if the scheduler was down for a long time.
    var referenceTime = nowInTz
    if lastRun.isSome and lastRun.get().inZone(tz) > nowInTz:
      referenceTime = lastRun.get().inZone(tz)

    var searchTime = referenceTime
    # If using lastRun as base, we add 1 sec. But if using NOW, we might include NOW?
    # Better to just add 1 sec to ensure strictly > referenceTime if referenceTime was a trigger.
    # But usually 'now' is not exactly on the second.
    # Safe to keep +1s? Or just start at referenceTime.
    # If referenceTime is 14:00:00 (exact) and interval is 5m.
    # We want 14:05:00.
    # If we start at 14:00:00, offset logic might pick 14:00:00.
    # ValidNext > lastRun check handles duplication.
    # Let's keep it simple: Start at 'now'.

    # If lastRun is close (e.g. 1m ago), using 'now' skips the immediate catchup.
    # But this is preferred to 5000 triggers.
    if lastRun.isSome:
      if referenceTime <= lastRun.get().inZone(tz):
        searchTime = lastRun.get().inZone(tz) + 1.seconds

    for i in 0..MaxSearchDays:
      let dayStart = dateTime(searchTime.year, searchTime.month,
          searchTime.monthday, 0, 0, 0, 0, tz)

      # Determine strict start/end for this day
      var wStart = dayStart
      var wEnd = dayStart + 1.days

      if hasWindow:
        wStart = dateTime(searchTime.year, searchTime.month,
            searchTime.monthday, startH, startM, 0, 0, tz)
        if (startH * 60 + startM) > (endH * 60 + endM):
          # Cross midnight not supported yet, fallback to end of day
          discard
        else:
          wEnd = dateTime(searchTime.year, searchTime.month,
              searchTime.monthday, endH, endM, 0, 0, tz)

      if not hasWindow:
        # Simple Interval: Aligned to lastRun + k*interval
        if lastRun.isSome:
          let diffMin = (searchTime - lastRun.get().inZone(tz)).inMinutes
          # We want next slot > searchTime
          # If alignment is required (e.g. strict 5m from start), we assume relative to Start?
          # But without Window, it's relative to First Run.
          # Let's just do: next = last + interval.
          # But if we drifted, we want multiple of interval.
          # Let's assume relative to 00:00?
          # Safety: Just last + interval if no window.
          # User specific issue was "Repeating job... start: 00:00". So they HAVE window/start.
          # If they don't have window, logic falls here.

          return some(lastRun.get().inZone(tz) + task.intervalMinutes.minutes)
        else:
          return some(nowInTz)

      else:
        # Aligned Logic within Window
        if searchTime <= wStart:
          searchTime = wStart

        let offsetMinutes = (searchTime - wStart).inMinutes

        var k = 0
        if offsetMinutes < 0:
          k = 0
        else:
          if offsetMinutes mod task.intervalMinutes == 0:
            k = (offsetMinutes div task.intervalMinutes).int
          else:
            k = (offsetMinutes div task.intervalMinutes).int + 1

        let nextT = wStart + (k * task.intervalMinutes).minutes

        # Ensure strict progression
        var validNext = nextT
        if lastRun.isSome and validNext <= lastRun.get().inZone(tz):
          validNext = validNext + task.intervalMinutes.minutes

        if validNext <= wEnd:
          if task.calendarPath == "" or isDateAllowed(task.dateList, validNext):
            return some(validNext)

        searchTime = dayStart + 1.days
        continue

    return none(DateTime)

  of stOnStart:
    # If we have server start time, check if we should trigger "now"
    if serverStartTime.isSome:
      let start = serverStartTime.get()
      # Runnable if never run OR last run < start
      if lastRun.isNone or lastRun.get() < start:
        return some(nowInTz) # Trigger effectively "now"
    return none(DateTime)

