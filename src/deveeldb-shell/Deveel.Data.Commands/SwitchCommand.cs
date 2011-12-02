using System;
using System.Collections;

using Deveel.Commands;
using Deveel.Shell;

namespace Deveel.Data.Shell {
	[Command("switch", ShortDescription = "switch to session with the given session name")]
	[CommandSynopsis("switch <session-name>")]
	[CommandGroup("sessions")]
	public sealed class SwitchCommand : Command {
		public override CommandResultCode Execute(object context, string[] args) {
			int argc = args.Length;
			string sessionName = null;

			DeveelDBShell shell = (DeveelDBShell)Application;

			if (argc != 1 && shell.SessionManager.SessionCount != 2)
				return CommandResultCode.SyntaxError;

			if (argc == 0 && shell.SessionManager.SessionCount == 2) {
				IEnumerator i = shell.SessionManager.SessionNames.GetEnumerator();
				while (i.MoveNext()) {
					sessionName = (String) i.Current;
					if (!sessionName.Equals(shell.SessionManager.CurrentSession.Name)) {
						break;
					}
				}
			} else {
				sessionName = args[0];
			}

			SqlSession session = shell.SessionManager.GetSessionByName(sessionName);
			if (session == null) {
				OutputDevice.Message.WriteLine("'" + sessionName + "': no such session");
				return CommandResultCode.ExecutionFailed;
			}
			shell.SessionManager.SetCurrentSession(session);
			return CommandResultCode.Success;
		}
	}
}