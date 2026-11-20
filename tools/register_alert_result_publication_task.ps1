param(
    [string]$TaskName = 'vivo-project Yield IJP Alert Publication',
    [switch]$Force
)

# Never reintroduce the obsolete 30-minute/logon task definition.
throw 'This registrar is retired. Use api\inspection\register_tasks.ps1 -Apply to install vivo-project Alert Inspection (07:15 and 19:15 daily) and the two weekly mail triggers.'
