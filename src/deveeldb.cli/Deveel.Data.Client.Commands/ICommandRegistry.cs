using System;
using System.Collections.Generic;

namespace Deveel.Data.Client.Commands {
	public interface ICommandRegistry {
		IEnumerable<string> CommandNames { get; }


		void RegisterCommand(ICommand command);

		void RegisterCommand(string name, Type commandType);

		ICommand ResolveCommand(string[] tokens);

		ICommand ResolveCommand(string commandName);

		void RemoveCommand(string commandName);
	}
}
