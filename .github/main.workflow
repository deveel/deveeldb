workflow "Build" {
  on = "push"
  resolves = ["Build and Test"]
}

action "Build and Test" {
  uses = "./.github/actions/dotnetcore-cli"
  runs = "build.sh"
}
