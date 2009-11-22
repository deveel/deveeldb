using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("Close", "&Close")]
	public sealed class CloseActiveChildCommand : Command {
		public override bool Enabled {
			get { return HostWindow.ActiveChild != null; }
		}

		public override void Execute() {
			Form child = HostWindow.ActiveChild;
			if (child != null)
				child.Close();
		}
	}
}