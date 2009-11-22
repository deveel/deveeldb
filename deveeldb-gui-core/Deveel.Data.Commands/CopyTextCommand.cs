using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("CopyText", "Copy")]
	[CommandShortcut(Keys.Control | Keys.C, "Ctrl+C")]
	[CommandImage("Deveel.Data.Images.page_copy.png")]
	public sealed class CopyTextCommand : Command {
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

			Clipboard.SetText(selectedText, TextDataFormat.Text);
		}

		#endregion
	}
}