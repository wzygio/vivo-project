Option Explicit
Dim shell, files, projectRoot, command, result
Set shell = CreateObject("WScript.Shell")
Set files = CreateObject("Scripting.FileSystemObject")
projectRoot = files.GetParentFolderName(WScript.ScriptFullName)
shell.CurrentDirectory = projectRoot
command = """" & files.BuildPath(projectRoot, "start_streamlit.bat") & """"
' Wait for startup verification, then return its real result to Task Scheduler.
result = shell.Run(command, 0, True)
WScript.Quit result
