import std/[times, strutils, options]

type
  CronField = object
    values: seq[int] # Allowed values for this field
    all: bool        # True if '*'

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

proc ensureProgress(prev: DateTime, cur: DateTime,
    step: Duration): DateTime =
  ## DST fall-back creates duplicate wall times (e.g. 02:00 occurs twice).
  ## Rebuilding a DateTime from wall components with `dateTime(...)` always
  ## resolves to the *first* occurrence, so `t + 1.hours` followed by a
  ## truncation to the start of the hour can move *backwards* in absolute
  ## time and loop forever. Force monotonic progress in UTC.
  result = cur
  var guard = 0
  while result.toTime <= prev.toTime:
    result = result + step
    inc guard
    if guard > 200:
      # Should never happen (duplicate hour needs at most 1-2 steps,
      # initial minute truncation at most ~60 steps for a 1h fallback).
      # Break anyway to guarantee termination; the outer year bound
      # will then raise ValueError.
      break

proc nextRun*(expression: CronExpr, after: DateTime): DateTime =
  ## Determine the next run time after the given time
  var t = after + 1.minutes
  # Reset seconds to 0
  t = dateTime(t.year, t.month, t.monthday, t.hour, t.minute, 0, 0, t.timezone)
  # The wall-clock rebuild above can resolve to the earlier occurrence of an
  # ambiguous time and end up <= `after` (fall-back). Push forward absolutely.
  t = ensureProgress(after, t, initDuration(minutes = 1))

  # Safety break designed to prevent infinite loops - e.g. look ahead up to 5 years
  let endYear = t.year + 5

  # Secondary iteration cap: with the monotonic-progress guard every iteration
  # advances UTC by >= 1 minute, so 5 years can never need more than ~2.7M
  # steps. Cap well above that to turn any future regression into an error
  # instead of a hang.
  var iter = 0
  const MaxIters = 5_000_000

  while t.year <= endYear:
    inc iter
    if iter > MaxIters:
      raise newException(ValueError, "No run time found within reasonable limit")

    if not matches(expression.month, t.month.int):
      let prev = t
      t = t + 1.months
      # Reset to start of month
      t = dateTime(t.year, t.month, 1, 0, 0, 0, 0, t.timezone)
      t = ensureProgress(prev, t, initDuration(days = 1))
      continue

    if not matches(expression.day, t.monthday):
      let prev = t
      t = t + 1.days
      # Reset to start of day
      t = dateTime(t.year, t.month, t.monthday, 0, 0, 0, 0, t.timezone)
      t = ensureProgress(prev, t, initDuration(days = 1))
      continue

    # Weekday check
    # Standard: 0=Sun, 1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri, 6=Sat.
    # Nim: dMon(0)..dSun(6).
    # dSun(6) -> 0, dMon(0) -> 1
    let nimDay = t.weekday
    let cronDay = if nimDay == dSun: 0 else: nimDay.int + 1

    if not matches(expression.weekday, cronDay):
      let prev = t
      t = t + 1.days
      t = dateTime(t.year, t.month, t.monthday, 0, 0, 0, 0, t.timezone)
      t = ensureProgress(prev, t, initDuration(days = 1))
      continue

    if not matches(expression.hour, t.hour):
      let prev = t
      t = t + 1.hours
      # Reset to start of hour
      t = dateTime(t.year, t.month, t.monthday, t.hour, 0, 0, 0, t.timezone)
      t = ensureProgress(prev, t, initDuration(hours = 1))
      continue

    if not matches(expression.minute, t.minute):
      t = t + 1.minutes
      continue

    return t

  raise newException(ValueError, "No run time found within reasonable limit")
