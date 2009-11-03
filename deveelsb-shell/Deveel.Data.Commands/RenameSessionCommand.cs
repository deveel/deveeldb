using System;

using Deveel.Commands;
using Deveel.Data.Shell;

namespace Deveel.Data.Commands {
	[Command("rename-session", ShortDescription = "rename current session. This influences the prompt")]
	[CommandSynopsis("rename-session <new-session-name>")]
	[CommandGroup("sessions")]
	public sealed class RenameSessionCommand : Command {
		public override CommandResultCode Execute(object context, string[] args) {
			int argc = args.Length;
			if (argc != 1)
				return CommandResultCode.SyntaxError;

			DeveelDBShell shell = (DeveelDBShell)Application;

			string sessionName = args[0];
			if (sessionName.Length < 1)
				return CommandResultCode.SyntaxError;

			return shell.SessionManager.RenameSession(shell.SessionManager.CurrentSession.Name, sessionName);
		}
	}
}