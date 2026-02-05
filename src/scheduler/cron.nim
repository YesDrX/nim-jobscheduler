import std/[times, strutils, options]

type
  CronField = object
    values: seq[int]  # Allowed values for this field
    all: bool         # True if '*'

  CronExpr* = object
    minute: CronField
    hour: CronField
    day: CronField
    month: CronField
    weekday: CronField

proc parseField(field: string, minVal, maxVal: int): CronField =
  result = CronField(values: @[], all: false)
  if field == "*":
    result.all = true
    for i in minVal..maxVal: result.values.add(i)
    return

  for part in field.split(','):
    if '/' in part:
      let stepParts = part.split('/')
      let step = parseInt(stepParts[1])
      var rangeStart, rangeEnd: int
      if stepParts[0] == "*":
        rangeStart = minVal
        rangeEnd = maxVal
      elif '-' in stepParts[0]:
        let rangeParts = stepParts[0].split('-')
        rangeStart = parseInt(rangeParts[0])
        rangeEnd = parseInt(rangeParts[1])
      else:
        rangeStart = parseInt(stepParts[0])
        rangeEnd = maxVal 
      
      var i = rangeStart
      while i <= rangeEnd:
        result.values.add(i)
        i += step
    elif '-' in part:
      let rangeParts = part.split('-')
      let start = parseInt(rangeParts[0])
      let stop = parseInt(rangeParts[1])
      for i in start..stop: result.values.add(i)
    else:
      result.values.add(parseInt(part))

proc parseCron*(expr: string): Option[CronExpr] =
  let parts = expr.splitWhitespace()
  if parts.len != 5:
    return none(CronExpr)

  try:
    result = some(CronExpr(
      minute: parseField(parts[0], 0, 59),
      hour: parseField(parts[1], 0, 23),
      day: parseField(parts[2], 1, 31),
      month: parseField(parts[3], 1, 12),
      weekday: parseField(parts[4], 0, 6) # 0=Sunday
    ))
  except ValueError:
    return none(CronExpr)

proc matches(field: CronField, val: int): bool =
  return field.all or val in field.values

proc nextRun*(expression: CronExpr, after: DateTime): DateTime =
  ## Determine the next run time after the given time
  var t = after + 1.minutes
  # Reset seconds to 0
  t = dateTime(t.year, t.month, t.monthday, t.hour, t.minute, 0, 0, t.timezone)
  
  # Safety break designed to prevent infinite loops - e.g. look ahead up to 5 years
  let endYear = t.year + 5

  while t.year <= endYear:
    if not matches(expression.month, t.month.int):
      t = t + 1.months
      # Reset to start of month
      t = dateTime(t.year, t.month, 1, 0, 0, 0, 0, t.timezone)
      continue

    if not matches(expression.day, t.monthday):
      t = t + 1.days
      # Reset to start of day
      t = dateTime(t.year, t.month, t.monthday, 0, 0, 0, 0, t.timezone)
      continue
    
    # Weekday check
    # Standard: 0=Sun, 1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri, 6=Sat.
    # Nim: dMon(0)..dSun(6).
    # dSun(6) -> 0, dMon(0) -> 1
    let nimDay = t.weekday
    let cronDay = if nimDay == dSun: 0 else: nimDay.int + 1
    
    if not matches(expression.weekday, cronDay):
      t = t + 1.days
      t = dateTime(t.year, t.month, t.monthday, 0, 0, 0, 0, t.timezone)
      continue

    if not matches(expression.hour, t.hour):
      t = t + 1.hours
      t = dateTime(t.year, t.month, t.monthday, t.hour, 0, 0, 0, t.timezone)
      continue

    if not matches(expression.minute, t.minute):
      t = t + 1.minutes
      continue

    return t

  raise newException(ValueError, "No run time found within reasonable limit")
