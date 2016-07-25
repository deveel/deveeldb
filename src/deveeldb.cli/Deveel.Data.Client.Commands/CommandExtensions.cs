using System;
using System.Collections.Generic;

namespace Deveel.Data.Client.Commands {
	public static class CommandExtensions {
		public static bool IsCompleting(this ICommand command) {
			return command is ICompleter;
		}

		public static IEnumerable<string> Complete(this ICommand command, string[] tokens, string currentToken) {
			return (command as ICompleter).Complete(tokens, currentToken);
		}

		public static bool IsOptionsHandler(this ICommand command) {
			return command is IOptionHandler;
		}

		public static void HandleOptions(this ICommand command, IOptions options) {
			(command as IOptionHandler).HandleOptions(options);
		}
	}
}
