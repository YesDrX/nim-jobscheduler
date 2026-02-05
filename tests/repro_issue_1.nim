import std/[times, options, unittest, strutils]
import ../src/models/types
import ../src/models/task
import ../src/scheduler/triggers

suite "Scheduling Logic Tests":

  test "Interval Alignment Bug":
    # Setup a task: Interval 5 min, Start 00:00, End 23:00
    var t = Task(
      name: "TestTask",
      scheduleType: stInterval,
      intervalMinutes: 5,
      intervalStart: "00:00",
      intervalEnd: "23:00",
      timezone: "UTC",
      enabled: true
    )

    # Mock Now: 13:02 (Misaligned)
    let now = dateTime(2025, mJan, 1, 13, 2, 0, 0, utc())

    # Case 1: First Run (LastRun = None)
    # Should run immediately? Or align to 13:00?
    # Current behavior: Runs immediately at 13:02.
    # Desired behavior: Should probably align to 13:00 or 13:05?
    # Actually, for first run, "now" is acceptable, but subsequent ones should align.

    # Case 2: Last Run was 12:00. (On grid)
    # We missed 12:05 ... 13:00.
    # Next trigger should be 13:00 (Catch up) or 13:05.
    # BUT MOST IMPORTANTLY: It must be on a :00 or :05 mark.

    let lastRun = some(dateTime(2025, mJan, 1, 12, 0, 0, 0, utc()))

    let nextOpt = getNextTrigger(t, now, lastRun)

    require(nextOpt.isSome)
    let next = nextOpt.get()

    echo "Next Trigger: ", next.format("HH:mm:ss")

    # Assert alignment
    check(next.minute mod 5 == 0)
    check(next.second == 0)

    # Assert reasonable time (should be <= now if catching up, or close to future)
    # If we catch up latest missed: 13:00.
    # If we catch up next future: 13:05.
    # In no case should it be 13:02.

    check(next.minute == 0 or next.minute == 5)

  test "Interval Alignment with Offset Start":
    # Interval 5 min, Start 00:03.
    # Grid: 00:03, 00:08, ...
    var t = Task(
     name: "TestTask",
     scheduleType: stInterval,
     intervalMinutes: 5,
     intervalStart: "00:03",
     intervalEnd: "23:00",
     timezone: "UTC",
     enabled: true
    )

    let now = dateTime(2025, mJan, 1, 13, 11, 0, 0, utc())
    # 13:11.
    # Grid near here: ... 13:03, 13:08, 13:13 ...
    # Last run: 12:03

    let lastRun = some(dateTime(2025, mJan, 1, 12, 3, 0, 0, utc()))

    let nextOpt = getNextTrigger(t, now, lastRun)
    require(nextOpt.isSome)
    let next = nextOpt.get()

    echo "Next Trigger Offset: ", next.format("HH:mm:ss")

    # integer math to check modulo
    let mins = next.minute
    # (mins - 3) % 5 == 0
    check((mins - 3) mod 5 == 0)

