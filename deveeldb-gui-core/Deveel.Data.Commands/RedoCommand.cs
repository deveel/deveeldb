using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("Redo", "&Redo")]
	[CommandShortcut(Keys.Control | Keys.Y, "Ctrl+Y")]
	[CommandImage("Deveel.Data.Images.arrow_redo.png")]
	public sealed class RedoCommand : Command {
		#region Overrides of Command

		public override bool Enabled {
			get {
				IEditor editor = Editor;
				return (editor != null && editor.SupportsHistory);
			}
		}

		public override void Execute() {
			IEditor editor = Editor;
			if (editor != null)
				editor.Redo();
		}

		#endregion
	}
}