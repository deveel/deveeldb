using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("DeleteText", "&Delete")]
	[CommandShortcut(Keys.Delete, "Del")]
	[CommandImage("Deveel.Data.Images.cross.png")]
	public sealed class DeleteTextCommand : Command {
		#region Overrides of Command

		public override bool Enabled {
			get {
				IEditor editor = Editor;
				return (editor != null && editor.SelectedText.Length > 0);
			}
		}

		public override void Execute() {
			IEditor editor = Editor;
			if (editor != null)
				editor.ClearSelectedText();
		}

		#endregion
	}
}