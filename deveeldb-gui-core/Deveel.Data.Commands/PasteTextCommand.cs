using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("PasteText", "Paste")]
	[CommandShortcut(Keys.Control | Keys.V, "Ctrl+V")]
	[CommandImage("Deveel.Data.Images.paste_plain.png")]
	public sealed class PasteTextCommand : Command {
		#region Overrides of Command

		public override bool Enabled {
			get {
				IEditor editor = Editor;
				return editor != null;
			}
		}

		public override void Execute() {
			IEditor editor = Editor;
			if (editor.SelectedText.Length > 0)
				editor.ClearSelectedText();

			string text = Clipboard.GetText(TextDataFormat.Text);
			editor.Insert(text);
		}

		#endregion
	}
}