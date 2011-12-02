using System;

using Deveel.Commands;
using Deveel.Data.Shell;
using Deveel.Design;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	[Command("status", RequiresContext = true, ShortDescription = "show status of the current session")]
	internal class StatusCommand : Command {
		public override CommandResultCode Execute(object context, string[] args) {
			SqlSession session = (SqlSession) context;
			OutputDevice.Message.WriteLine("Connection String:    " + session.ConnectionString);
			OutputDevice.Message.Write("uptime: ");
			TimeRenderer.PrintTime((long)session.Uptime.TotalMilliseconds, OutputDevice.Message);
			OutputDevice.Message.WriteLine("; statements: " + session.StatementCount);
			return CommandResultCode.Success;
		}
	}
}