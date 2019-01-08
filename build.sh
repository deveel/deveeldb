#!/bin/sh -l

sh -c "dotnet restore"
sh -c "dotnet build -c %CONFIGURATION% /p:GeneratePackageOnBuild=true /p:Version=%APPVEYOR_BUILD_VERSION% /p:DebugType=Full"
sh -c "dotnet tool install coveralls.net --version 1.0.0 --tool-path tools"
sh -c "dotnet test /p:CollectCoverage=true /p:CoverletOutputFormat=opencover /p:Exclude="[*.tests?]*" --no-build"