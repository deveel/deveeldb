using System;
using System.Collections;

namespace Deveel.Data.Commands {
	public sealed class CommandHandler {
		public CommandHandler(IApplicationServices services) {
			commands = new Hashtable();
			this.services = services;
		}

		private readonly IApplicationServices services;
		private readonly Hashtable commands;

		public IApplicationServices Services {
			get { return services; }
		}

		public ICommand GetCommand(Type commandType) {
			if (!typeof(ICommand).IsAssignableFrom(commandType))
				throw new ArgumentException();

			ICommand command = commands[commandType] as ICommand;
			if (command == null) {
				command = (ICommand) Activator.CreateInstance(commandType, true);
				if (command is Command) {
					((Command)command).SetServices(services);
					((Command)command).SetSettings(services.Settings);
				}

				commands[commandType] = command;
			}

			return command;
		}

		public ICommand GetCommand(string typeName) {
			foreach (DictionaryEntry entry in commands) {
				Type type = (Type) entry.Key;
				if (type.Name == typeName ||
					type.FullName == typeName)
					return entry.Value as ICommand;
			}

			return null;
		}

		public ICommand GetCommandByPartialName(string commandName) {
			if (!commandName.EndsWith("Command"))
				commandName += "Command";

			foreach (DictionaryEntry entry in commands) {
				Type type = (Type) entry.Key;
				if (type.Name.EndsWith(commandName))
					return (ICommand) entry.Value;
			}

			return null;
		}
	}
}