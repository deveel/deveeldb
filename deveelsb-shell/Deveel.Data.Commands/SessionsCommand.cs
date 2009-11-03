using System;

using Deveel.Commands;
using Deveel.Data.Shell;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	[Command("sessions", ShortDescription = "lists all the sessions stored.")]
	[CommandGroup("sessions")]
	public sealed class SessionsCommand : Command {
		public override CommandResultCode Execute(object context, string[] args) {
			int argc = args.Length;
			if (argc != 0)
				return CommandResultCode.SyntaxError;

			DeveelDBShell application = (DeveelDBShell)Application;
			application.OutputDevice.WriteLine("current session is marked with '*'");
			application.Connections.RenderTable(application.OutputDevice);
			return CommandResultCode.Success;
		}
	}
}