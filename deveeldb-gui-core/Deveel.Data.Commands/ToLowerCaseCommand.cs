using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[CommandSmallImage("Deveel.Data.Images.text_lowercase.png")]
	public sealed class ToLowerCaseCommand : Command {
		public ToLowerCaseCommand() 
			: base("Convert to lower case", Keys.Control | Keys.U, null) {
		}

		public override bool Enabled {
			get { return Editor != null; }
		}

		public override void Execute() {
			IEditor editor = Editor;
			if (editor != null &&
				(editor.SelectedText != null && editor.SelectedText.Length > 0))
				editor.Insert(editor.SelectedText.ToLower());
		}
	}
}