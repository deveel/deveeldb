using System;
using System.Collections.Generic;
using System.Linq;

using DryIoc;

namespace Deveel.Data.Client.Commands {
	public class DryIocCommandRegistry : ICommandRegistry {
		private Container container;

		public DryIocCommandRegistry(Container container) {
			this.container = container;
		}

		public DryIocCommandRegistry()
			: this(new Container()) {
		}

		public IEnumerable<string> CommandNames {
			get { return container.GetServiceRegistrations().Select(x => (string) x.OptionalServiceKey); }
		}

		public void RegisterCommand(ICommand command) {
			if (command == null)
				return;

			if (command.CommandInfo == null)
				throw new ArgumentException();

			var name = command.CommandInfo.Name;
			container.RegisterInstance(command, serviceKey:name);
		}

		public void RegisterCommand(string name, Type commandType) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");
			if (commandType == null)
				throw new ArgumentNullException("commandType");
			if (!typeof(ICommand).IsAssignableFrom(commandType))
				throw new ArgumentException(String.Format("Type '{0}' is not assignable from '{0}'.", commandType, typeof(ICommand)));

			container.Register(commandType, serviceKey:name);
		}

		public ICommand ResolveCommand(string[] tokens) {
			var commands = container.ResolveMany<ICommand>();
			foreach (var command in commands) {
				if (command.Matches(tokens))
					return command;
			}

			return null;
		}

		public ICommand ResolveCommand(string commandName) {
			if (String.IsNullOrEmpty(commandName))
				throw new ArgumentNullException("commandName");

			return container.Resolve<ICommand>(commandName);
		}

		public void RemoveCommand(string commandName) {
			container.Unregister<ICommand>(commandName);
		}
	}
}
