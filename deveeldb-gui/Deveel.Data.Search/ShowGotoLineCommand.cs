using System;
using System.Windows.Forms;

using Deveel.Data.Commands;

namespace Deveel.Data.Search {
	[Command("GotoLine", "Got to line ...")]
	[CommandShortcut(Keys.Control | Keys.G)]
	public sealed class ShowGotoLineCommand : Command {
		#region Overrides of Command

		public override bool Enabled {
			get { return HostWindow.ActiveChild is IBrowsableDocument; }
		}

		public override void Execute() {
			if (Enabled) {
				GotoLineForm form = (GotoLineForm) Services.Resolve(typeof(GotoLineForm));
				form.ShowDialog(HostWindow as Form);
			}
		}

		#endregion
	}
}