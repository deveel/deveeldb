using System;
using System.Windows.Forms;

using Deveel.Data.Commands;
using Deveel.Data.Plugins;

namespace Deveel.Data.ConnectionStrings {
	public sealed class ConnectionStringsPlugin : Plugin {
		public ConnectionStringsPlugin()
			: base("Connection Strings", "Manages the stored connection strings", 10) {	
		}

		#region Overrides of Plugin

		public override void Init() {
			Services.RegisterComponent("ConnectionStringForm", typeof(ConnectionStringForm));
			ToolStripMenuItem editMenu = Services.HostWindow.GetMenuItem("edit");

			CommandControlBuilder controlBuilder = new CommandControlBuilder(Services.CommandHandler);
			editMenu.DropDownItems.Add(controlBuilder.CreateToolStripMenuItem(typeof(ConnectionStringsEditCommand)));
			Services.HostWindow.AddToolStripCommand(-1, typeof(ConnectionStringsEditCommand));
		}

		#endregion
	}
}