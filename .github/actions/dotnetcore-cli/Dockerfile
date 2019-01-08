FROM microsoft/dotnet:sdk

LABEL version="1.0.0"

LABEL maintainer="Microsoft Corporation"
LABEL com.github.actions.name=".NET Core CLI"
LABEL com.github.actions.description="GitHub Action to build, test, package or publish a dotnet application, or run a custom dotnet command."
LABEL com.github.actions.icon="triange"
LABEL com.github.actions.color="blue"

COPY . .

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]