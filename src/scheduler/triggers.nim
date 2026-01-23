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

proc checkTimeOfDay(task: Task, now: DateTime, lastRun: Option[DateTime]): bool =
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
      if lastInTz.year == nowInTz.year and lastInTz.month == nowInTz.month and lastInTz.monthday == nowInTz.monthday:
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
    if scanf(task.intervalStart, "$i:$i", startH, startM) and scanf(task.intervalEnd, "$i:$i", endH, endM):
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

proc shouldTrigger*(task: Task, now: DateTime, lastRun: Option[DateTime], serverStartTime: DateTime): bool =
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

proc getNextTrigger*(task: Task, now: DateTime, lastRun: Option[DateTime], serverStartTime: Option[DateTime] = none(DateTime)): Option[DateTime] =
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
    var checkTime = dateTime(currentDay.year, currentDay.month, currentDay.monthday, h, m, 0, 0, tz)
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
    
    var baseTime = nowInTz
    if lastRun.isSome:
       let lastInTz = lastRun.get().inZone(tz)
       baseTime = lastInTz + task.intervalMinutes.minutes
       if baseTime < nowInTz: baseTime = nowInTz
    else:
       baseTime = nowInTz

    # Check Window logic + Calendar
    # With interval, moving forward is tricky because interval depends on previous run.
    # BUT if we are "predicting" next run, user usually wants "When will it run?" 
    # If Calendar blocks today, does it run tomorrow at start time? Or does it skip?
    # Usually skips to next valid day's Window Start.
    
    # Loop days
    var searchTime = baseTime
    # Align searchTime to proper start window if needed
    
    # Logic:
    # 1. Check if searchTime is valid (Calendar & Window)
    # 2. If not, advance to next valid moment
    
    var startH, startM, endH, endM: int
    let hasWindow = (task.intervalStart != "" and task.intervalEnd != "") and 
                    scanf(task.intervalStart, "$i:$i", startH, startM) and 
                    scanf(task.intervalEnd, "$i:$i", endH, endM)

    for i in 0..MaxSearchDays:
       # Check Calendar
       # Logic flipped: if Allowed, proceed.
       let allowed = if task.calendarPath == "": true 
                     else: isDateAllowed(task.dateList, searchTime)
                     
       if allowed:
          # Check Window
          if hasWindow:
             let startMins = startH * 60 + startM
             let endMins = endH * 60 + endM
             let curMins = searchTime.hour * 60 + searchTime.minute
             var inWindow = false
             if startMins <= endMins: inWindow = curMins >= startMins and curMins <= endMins
             else: inWindow = (curMins >= startMins) or (curMins <= endMins)
             
             if inWindow:
                return some(searchTime)
             else:
                # Move to next Start
                var nextStart = dateTime(searchTime.year, searchTime.month, searchTime.monthday, startH, startM, 0, 0, tz)
                
                if startMins <= endMins:
                    if curMins > endMins: nextStart += 1.days
                    # if curMins < startMins, nextStart is this day, later.
                else: 
                    # Cross midnight
                    # Invalid gap is (endMins .. startMins)
                    if curMins > endMins and curMins < startMins:
                        # Move to startMins today
                        discard
                
                if nextStart < searchTime: nextStart += 1.days
                searchTime = nextStart
                continue # Re-check this new time (it might be next day, so check calendar again)
                
          else:
             # No window, just calendar check passed
             return some(searchTime)
       
       # Calendar invalid, move to next day (Start of day or Start Window)
       # Reset to next day 00:00 or Window Start
       var nextDay = searchTime + 1.days
       if hasWindow:
          nextDay = dateTime(nextDay.year, nextDay.month, nextDay.monthday, startH, startM, 0, 0, tz)
       else:
          # Just ensure we move forward? Interval logic suggests we just wait. 
          # But if blocked by calendar, we probably reset to "Start of next valid day"?
          # Or preserve time? 
          # "Test Calendar" implying banking holidays usually means pause operation.
          # Resetting to 00:00 or start of day seems reasonably safe for Interval tasks.
          nextDay = dateTime(nextDay.year, nextDay.month, nextDay.monthday, 0, 0, 0, 0, tz)
          
       searchTime = nextDay
       if searchTime < nowInTz: searchTime = nowInTz # Safety
       
       searchTime = nextDay
       if searchTime < nowInTz: searchTime = nowInTz # Safety
       
    return candidate

  of stOnStart:
    # If we have server start time, check if we should trigger "now"
    if serverStartTime.isSome:
       let start = serverStartTime.get()
       # Runnable if never run OR last run < start
       if lastRun.isNone or lastRun.get() < start:
          return some(nowInTz) # Trigger effectively "now"
    return none(DateTime)

