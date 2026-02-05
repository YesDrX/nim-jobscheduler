import scheduler/cron
import std/[times, options, unittest]

suite "Cron Parser":
  test "Parse simple cron":
    let c = parseCron("*/5 * * * *")
    check c.isSome
  
  test "Next run calculation":
    let c = parseCron("0 12 * * *").get() # Every day at 12:00
    let now = dateTime(2023, mJan, 1, 10, 0, 0, 0, utc())
    let next = c.nextRun(now)
    check next.hour == 12
    check next.minute == 0
    check next.monthday == 1

  test "Complex schedule":
    let c = parseCron("5 0 * * *").get() # At 00:05
    let now = dateTime(2023, mJan, 1, 0, 0, 0, 0, utc())
    let next = c.nextRun(now)
    # With 00:00 as start, next should be 00:05 today
    check next.hour == 0
    check next.minute == 5
    check next.monthday == 1
    
    # If it's already 00:06, it should be tomorrow
    let later = dateTime(2023, mJan, 1, 0, 6, 0, 0, utc())
    let nextLater = c.nextRun(later)
    check nextLater.monthday == 2
    check nextLater.hour == 0
    check nextLater.minute == 5

  test "Weekday":
    # 0 12 * * 1 (Every Monday at 12:00)
    let c = parseCron("0 12 * * 1").get() 
    # Jan 1 2023 was a Sunday. So next Mon is Jan 2.
    let now = dateTime(2023, mJan, 1, 10, 0, 0, 0, utc())
    let next = c.nextRun(now)
    check next.monthday == 2
    check next.weekday == dMon
