workflow "Build" {
  on = "push"
  resolves = ["test"]
}

action "restore" {
  uses = "./.github/actions/dotnetcore-cli"
  args = "restore"
}

action "test" {
  uses = "./.github/actions/dotnetcore-cli"
  args = "test"
  needs = ["restore"]
}
