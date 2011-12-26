@echo off
IF "%~1" == "mono" GOTO MonoCompile ELSE GOTO DotNetCompile

:DotNetCompile
"%~dp0..\nant\nant.exe" -buildfile:"%~dp0package.build" -D:lib="%~dp0..\lib" -D:deveeldb.root=%~dp0..\ -D:package=%1 package
GOTO End

:MonoCompile
SHIFT
mono "%~dp0..\nant\nant.exe" -buildfile:"%~dp0package.build" -D:lib="%~dp0..\lib" -D:deveeldb.root=%~dp0..\ -D:package=%1 package
GOTO End

:End