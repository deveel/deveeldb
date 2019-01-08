workflow "Build" {
  on = "push"
  resolves = ["test"],
}

action "restore" {
  uses = "./.github/actions/dotnetcore-cli"
  args = "restore"
}

action "build" {
  uses = "./.github/actions/dotnetcore-cli"
  needs = ["restore"]
  args = "build /p:GeneratePackageOnBuild=true /p:VersionPostfix=$GITHUB_SHA /p:DebugType=Full"
}

action "install coveralls.net" {
  uses = "./.github/actions/dotnetcore-cli"
  needs = ["build"]
  args = "tool install coveralls.net --version 1.0.0 --tool-path tools"
}

action "test" {
  uses = "./.github/actions/dotnetcore-cli"
  args = "test /p:CollectCoverage=true /p:CoverletOutputFormat=opencover /p:Exclude=\"[*.tests?]*\" --no-build\""
  needs = ["install coveralls.net"]
}
