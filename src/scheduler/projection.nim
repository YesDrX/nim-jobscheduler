import std/[times, options, strutils, strscans, sequtils]
import ../models/types
import ../models/task
import cron
import triggers
import timezones

type
  ProjectionResult* = object
    triggers*: seq[DateTime]
    summary*: string
    isHighFrequency*: bool

# Helper to get timezone from string (duplicated from triggers.nim for now or could export)
proc parseTimezone(tzName: string): Timezone =
  if tzName == "UTC": return utc()
  if tzName == "" or tzName == "Local": return local()
  try: return timezones.tz(tzName)
  except: return utc()

proc projectTask*(task: Task, date: DateTime): ProjectionResult =
  result = ProjectionResult(triggers: @[], summary: "", isHighFrequency: false)
  
  if not task.enabled:
    result.summary = "Disabled"
    return result

  let tz = parseTimezone(task.timezone)
  # Ensure date is start of day in task's timezone
  let startOfDay = dateTime(date.year, date.month, date.monthday, 0, 0, 0, 0, tz)
  let endOfDay = startOfDay + 1.days
  
  # Check Calendar Blocking First (for entire day)
  if task.calendarPath != "" and not isDateAllowed(task.dateList, startOfDay):
    result.summary = "Blocked by Calendar"
    return result

  case task.scheduleType:
  of stCron:
    if task.cronExpr == "": return
    let cronOpt = parseCron(task.cronExpr)
    if cronOpt.isNone: 
      result.summary = "Invalid Cron"
      return

    var next = startOfDay
    # Adjust next to be slightly before startOfDay so nextRun(startOfDay) works if it is exactly startOfDay?
    # cron.nextRun(t) returns > t. 
    # So to capture a run AT 00:00, we need to pass (startOfDay - 1.second)
    # But triggers returns next run AFTER time.
    
    var searchTime = startOfDay - 1.seconds
    let ce = cronOpt.get()
    
    var count = 0
    while true:
      try:
        let t = ce.nextRun(searchTime)
        if t >= endOfDay: break
        
        result.triggers.add(t)
        searchTime = t
        count.inc()
        
        if count > 50:
           result.isHighFrequency = true
           result.summary = "More than 50 runs (Cron: " & task.cronExpr & ")"
           result.triggers = @[] # Clear list to save bandwidth
           break
           
      except:
        break
        
    if not result.isHighFrequency and result.triggers.len == 0:
        result.summary = "No runs scheduled for this day"

  of stTime:
    if task.timeOfDay == "": return
    var h, m: int
    if scanf(task.timeOfDay, "$i:$i", h, m):
       let t = dateTime(startOfDay.year, startOfDay.month, startOfDay.monthday, h, m, 0, 0, tz)
       result.triggers.add(t)
    else:
       result.summary = "Invalid Time Format"

  of stInterval:
    if task.intervalMinutes <= 0: 
       result.summary = "Invalid Interval"
       return
       
    # Interval is tricky because it depends on previous run.
    # We will show "Window" or "Starting from X".
    # For projection, we assume ideal start at Window Start OR 00:00.
    
    var startH, startM, endH, endM: int
    let hasWindow = (task.intervalStart != "" and task.intervalEnd != "") and 
                    scanf(task.intervalStart, "$i:$i", startH, startM) and 
                    scanf(task.intervalEnd, "$i:$i", endH, endM)
    
    if result.isHighFrequency or task.intervalMinutes < 15:
       result.isHighFrequency = true
       if hasWindow:
          result.summary = "Every " & $task.intervalMinutes & "m between " & task.intervalStart & " and " & task.intervalEnd
       else:
          result.summary = "Every " & $task.intervalMinutes & "m all day"
    else:
       # Calculate discrete times
       var t = startOfDay
       if hasWindow:
          t = dateTime(startOfDay.year, startOfDay.month, startOfDay.monthday, startH, startM, 0, 0, tz)
          
       let winEnd = if hasWindow: 
           dateTime(startOfDay.year, startOfDay.month, startOfDay.monthday, endH, endM, 0, 0, tz)
           else: endOfDay
           
       # Handle midnight crossing window? Simplification: assume intra-day window
       # If endH < startH, it ends next day. We only care about THIS day.
       # So valid range is [start..Midnight] AND [00:00..end].
       # For now, simplistic start->end within day.
       
       while t < endOfDay:
           # Check window
           var inWindow = true
           if hasWindow:
              let curMins = t.hour * 60 + t.minute
              let startMins = startH * 60 + startM
              let endMins = endH * 60 + endM
              if startMins <= endMins:
                  inWindow = curMins >= startMins and curMins <= endMins
              else:
                  inWindow = (curMins >= startMins) or (curMins <= endMins)
           
           if inWindow:
             result.triggers.add(t)
             
           t = t + task.intervalMinutes.minutes
           
           if result.triggers.len > 50:
              result.isHighFrequency = true
              result.summary = "High frequency interval (" & $task.intervalMinutes & "m)"
              result.triggers = @[]
              break

  of stOnStart:
    result.summary = "Runs on server start"
