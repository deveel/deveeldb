workflow "Build" {
  on = "push"
  resolves = ["GitHub Action for Azure"]
}

action "GitHub Action for Azure" {
  uses = "actions/azure@4919f1449100fb0e6111a52466de7f2a1dc861dc"
  runs = "dotnet"
  args = "restore"
}
