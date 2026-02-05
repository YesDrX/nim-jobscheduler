import smtp, strutils, times, asyncdispatch, strformat, logging
import ../models/job
import ../models/execution
import ../config/types
import ../executor/logger

proc sendAlert*(jobId: int, job: Job, execId: int, execution: Execution,
    config: SmtpConfig, externalUrl : string) {.async.} =
  if not config.enabled: return

  if config.host == "":
    warn "[Alert] SMTP enabled but host missing"
    return

  let subject = "Job Failed: " & job.name
  let body = fmt"""
Job Scheduler Alert
===================

Job Name: {job.name}
Job ID: {jobId}
Execution ID: {execId}
Status: {execution.status}
Start Time: {execution.startTime}
End Time: {execution.endTime}
Log File: {execution.logFile}
URL: http://{externalUrl}/executions/{execId}/log

===================
Tail of Log File:
===================
...

{getLogContent(execution.logFile, 30)}

"""
  let msg = createMessage(
    subject,
    body,
    sender = config.fromAddr,
    mTo = config.toAddrs
  )
  try:
    let client = newSmtp(useSsl = config.useSSL)
    client.connect(config.host, Port(config.port))
    if config.fromAddr != "" and config.password != "":
      client.auth(config.fromAddr, config.password)
    client.sendmail(
      config.fromAddr,
      config.toAddrs,
      $msg
    )
    client.close()
    info "[Jobscheduler Alert] Email sent for job: " & job.name
  except Exception as e:
    info "[Jobscheduler Alert] Failed to send email: " & e.msg
