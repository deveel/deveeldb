using System;
using System.Data;

namespace Deveel.Data.Commands {
	[Command("CloseConnection", "Close Current Connection")]
	[CommandImage("Deveel.Data.Images.disconnect.png")]
	public sealed class CloseConnectionCommand : Command {
		public override bool Enabled {
			get {
				if (Settings.Connection == null)
					return false;
				return (Settings.Connection.State != ConnectionState.Broken &&
				        Settings.Connection.State != ConnectionState.Closed);
			}
		}

		public override void Execute() {
			Settings.CloseConnection();
		}
	}
}