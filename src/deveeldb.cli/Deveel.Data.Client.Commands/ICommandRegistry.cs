using System;

namespace Deveel.Data.Client.Commands {
	public interface ICommandRegistry {
		void RegisterCommand(ICommand command);

		ICommand ResolveCommand(string[] tokens);

		ICommand ResolveCommand(string commandName);

		void RemoveCommand(string commandName);
	}
}
