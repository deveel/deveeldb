using System;
using System.Globalization;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("ToTitle", "Convert to 'Title Case'")]
	[CommandShortcut(Keys.Control | Keys.Alt | Keys.U, "Ctrl+Alt+U")]
	[CommandImage("Deveel.Data.Images.text_dropcaps.png")]
	public sealed class ToTitleCaseCommand : Command {
		#region Overrides of Command

		public override bool Enabled {
			get { return HostWindow.ActiveChild as IEditor != null; }
		}

		public override void Execute() {
			IEditor editor = Editor;
			string text = editor.SelectedText;
			if (Enabled && text.Length > 0)
				editor.Insert(CultureInfo.CurrentCulture.TextInfo.ToTitleCase(text));
		}

		#endregion
	}
}