using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	public sealed class CloseChildrenCommand : Command {
		public CloseChildrenCommand() 
			: base("Close &All Windows") {
		}

		public override bool Enabled {
			get { return HostWindow.Form.MdiChildren.Length > 0; }
		}

		public override void Execute() {
			Form[] children = HostWindow.Form.MdiChildren;
			if (children.Length == 0)
				return;

			for (int i = 0; i < children.Length; i++) {
				Form child = children[i];
				Application.DoEvents();
				child.Close();
			}
		}
	}
}