using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("SelectAll", "Select &All")]
	[CommandShortcut(Keys.Control | Keys.A, "Ctrl+A")]
	public sealed class SelectAllTextCommand : Command {
		#region Overrides of Command

		public override bool Enabled {
			get {
				IEditor editor = Editor;
				return (editor != null);
			}
		}

		public override void Execute() {
			//TODO:
			throw new NotImplementedException();
		}

		#endregion
	}
}