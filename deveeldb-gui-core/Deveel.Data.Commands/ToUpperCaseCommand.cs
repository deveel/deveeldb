using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[CommandSmallImage("Deveel.Data.Images.text_uppercase.png")]
	public sealed class ToUpperCaseCommand : Command {
		public ToUpperCaseCommand() 
			: base("Convert to UPPER CASE", Keys.Control | Keys.Shift | Keys.U, null) {
		}

		public override bool Enabled {
			get { return Editor != null; }
		}

		public override void Execute() {
			IEditor editor = Editor;
			if (editor != null && 
				(editor.SelectedText != null && editor.SelectedText.Length > 0)) {
				editor.Insert(editor.SelectedText.ToUpper());
			}
		}
	}
}