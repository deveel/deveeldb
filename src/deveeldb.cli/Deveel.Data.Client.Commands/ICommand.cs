using System;

namespace Deveel.Data.Client.Commands {
	public interface ICommand {
		CommandInfo CommandInfo { get; }

		bool Matches(string[] tokens);

		CommandExecutionResult Execute(string commandText);
	}
}
