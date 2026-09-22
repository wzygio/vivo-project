Option Explicit
Dim shell, files, scriptPath, command, result
Set shell = CreateObject("WScript.Shell")
Set files = CreateObject("Scripting.FileSystemObject")
scriptPath = files.BuildPath(files.GetParentFolderName(WScript.ScriptFullName), "start_streamlit.bat")
command = """" & shell.ExpandEnvironmentStrings("%ComSpec%") & """ /d /s /c """"" & scriptPath & """"""
' Wait for startup verification, not for the lifetime of the server.
result = shell.Run(command, 0, True)
WScript.Quit result
