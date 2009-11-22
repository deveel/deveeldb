using System;

using Deveel.Data.Commands;

namespace Deveel.Data.ConnectionStrings {
	[Command("ConnectionEdit", "&Edit Connection Strings")]
	[CommandImage("Deveel.Data.Images.database_edit.png")]
	public sealed class ConnectionStringsEditCommand : Command {
		#region Overrides of Command

		public override void Execute() {
			ConnectionStringForm form = (ConnectionStringForm) Services.Resolve(typeof(ConnectionStringForm));
			form.ShowDialog(HostWindow.Form);
		}

		#endregion
	}
}