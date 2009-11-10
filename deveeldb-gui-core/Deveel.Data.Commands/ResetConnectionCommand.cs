using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[CommandSmallImage("Deveel.Data.Images.database_refresh.png")]
	public sealed class ResetConnectionCommand : Command {
		public ResetConnectionCommand()
			: base("&Reset Connection") {
		}

		public override bool Enabled {
			get { return Settings.Connection != null; }
		}

		public override void Execute() {
			try {
				HostWindow.SetPointerState(Cursors.WaitCursor);
				Settings.ResetConnection();
				HostWindow.SetStatus(null, "Connection reset");
			} finally {
				HostWindow.SetPointerState(Cursors.Default);
			}
		}
	}
}