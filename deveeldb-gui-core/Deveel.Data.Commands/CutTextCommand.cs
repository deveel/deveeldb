using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("CutText", "Cut")]
	[CommandShortcut(Keys.Control | Keys.X, "Ctrl+X")]
	[CommandImage("Deveel.Data.Images.cut.png")]
	public sealed class CutTextCommand : Command {
		#region Overrides of Command

		public override bool Enabled {
			get {
				IEditor editor = Editor;
				return (editor != null && editor.SelectedText.Length > 0);
			}
		}

		public override void Execute() {
			IEditor editor = Editor;
			if (editor == null)
				return;

			string selectedText = editor.SelectedText;
			if (selectedText.Length == 0)
				return;

			editor.ClearSelectedText();
			Clipboard.SetText(selectedText, TextDataFormat.Text);
		}

		#endregion
	}
}