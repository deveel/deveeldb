using System;
using System.Collections.Generic;

namespace Deveel.Data.Client.Commands {
	public static class CommandRegistry {
		private static ICommandRegistry current;

		static CommandRegistry() {
			Current = new EmptyCommandRegistry();
		}

		public static ICommandRegistry Current {
			get { return current; }
			set {
				if (value == null)
					value = new EmptyCommandRegistry();

				current = value;
			}
		}

		public static void RegisterCommand<TCommand>(this ICommandRegistry registry, string name)
			where TCommand : class, ICommand {
			registry.RegisterCommand(name, typeof(TCommand));
		}

		public static TCommand ResolveCommand<TCommand>(this ICommandRegistry registry, string name)
			where TCommand : class, ICommand {
			return (TCommand) registry.ResolveCommand(name);
		}

		public static void RegisterCommand<TCommand>(this ICommandRegistry registry, TCommand command)
			where TCommand : class, ICommand {
			registry.RegisterCommand(command);
		}

		#region EmptyCommandRegistry

		class EmptyCommandRegistry : ICommandRegistry {
			public IEnumerable<string> CommandNames {
				get { return new string[0]; }
			}

			public void RegisterCommand(ICommand command) {
			}

			public void RegisterCommand(string name, Type commandType) {
			}

			public ICommand ResolveCommand(string[] tokens) {
				return null;
			}

			public ICommand ResolveCommand(string commandName) {
				return null;
			}

			public void RemoveCommand(string commandName) {
			}
		}

		#endregion
	}
}
