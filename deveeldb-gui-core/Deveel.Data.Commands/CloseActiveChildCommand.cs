using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	public sealed class CloseActiveChildCommand : Command {
		public CloseActiveChildCommand() 
			: base("&Close") {
		}

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