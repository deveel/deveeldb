@echo off
IF "%~1" == "mono" GOTO MonoCompile ELSE GOTO DotNetCompile

:DotNetCompile
"%~dp0..\nant\nant.exe" -buildfile:"%~dp0deveeldb.build" -D:deveeldb.root=%~dp0 %*
GOTO End

:MonoCompile
SHIFT
mono "%~dp0..\nant\nant.exe" -buildfile:"%~dp0deveeldb.build" -D:deveeldb.root=%~dp0 %*
echo %1
GOTO End

:End