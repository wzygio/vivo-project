param(
    [string]$TaskName = 'vivo-project Yield IJP Alert Publication',
    [switch]$Force
)

# Never reintroduce the obsolete 30-minute/logon task definition.
throw 'This registrar is retired. Use api\inspection\register_tasks.ps1 -MailConfigPath <private-mail-yaml> -Apply to install daily 07:15 inspection and the two weekly mail triggers.'
