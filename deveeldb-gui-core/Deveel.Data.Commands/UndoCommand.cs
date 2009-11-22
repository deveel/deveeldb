using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("Undo", "&Undo")]
	[CommandShortcut(Keys.Control | Keys.Z, "Ctrl+Z")]
	[CommandImage("Deveel.Data.Images.arrow_undo.png")]
	public sealed class UndoCommand : Command {
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
				editor.Undo();
		}

		#endregion
	}
}