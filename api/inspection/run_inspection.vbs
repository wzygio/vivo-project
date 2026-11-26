Option Explicit
Dim fs, shell, root, interpreter
Set fs = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")
root = fs.GetParentFolderName(fs.GetParentFolderName(fs.GetParentFolderName(WScript.ScriptFullName)))
shell.CurrentDirectory = root
interpreter = fs.BuildPath(root, ".venv\Scripts\python.exe")
WScript.Quit shell.Run(Chr(34) & interpreter & Chr(34) & " -m api.inspection", 0, True)
