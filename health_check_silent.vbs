' Silent wrapper for health_check_flask.ps1
' Invoked by the "Stock Analyzer" scheduled task. wscript.exe has no
' console of its own, and WScript.Shell.Run with visibility=0 hides
' the launched PowerShell completely — bypasses Windows Terminal / conhost
' flashing that happens when powershell.exe is executed directly.
Set shell = CreateObject("WScript.Shell")
shell.Run "powershell.exe -NoProfile -ExecutionPolicy Bypass -File ""C:\xampp\htdocs\Claude\health_check_flask.ps1""", 0, False
