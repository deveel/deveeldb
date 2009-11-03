using System;

using Deveel.Commands;
using Deveel.Data.Shell;

namespace Deveel.Data.Commands {
	[Command("disconnect", RequiresContext = true, ShortDescription = "disconnects the current session")]
	[CommandSynopsis("disconnect")]
	[CommandGroup("sessions")]
	public sealed class DisconnectCommand : Command {
		public override string LongDescription {
			get {
				return "\tdisconnect current session. You can leave a session as\n"
				       + "\twell if you just type CTRL-D";
			}
		}

		public override CommandResultCode Execute(object context, string[] args) {
			int argc = args.Length;
			if (argc != 0)
				return CommandResultCode.SyntaxError;

			DeveelDBShell shell = (DeveelDBShell)Application;

			shell.SessionManager.CloseCurrentSession();
			shell.MessageDevice.WriteLine("session closed.");

			if (shell.SessionManager.HasSessions) {
				string currentSessionName = shell.SessionManager.FirstSessionName;
				SqlSession session = shell.SessionManager.GetSessionByName(currentSessionName);
				shell.SessionManager.SetCurrentSession(session);
				shell.SetPrompt(currentSessionName + "> ");
			}

			return CommandResultCode.Success;
		}
	}
}