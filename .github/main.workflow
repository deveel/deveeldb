workflow "Build" {
  on = "push"
  resolves = [".NET Core CLI"]
}

action ".NET Core CLI" {
  uses = "./.github/actions/dotnetcore-cli"
  args = "restore"
}
