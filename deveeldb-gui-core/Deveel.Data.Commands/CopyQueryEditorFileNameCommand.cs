using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("CopyFileName", "Copy Filename")]
	public class CopyQueryEditorFileNameCommand : Command {
		public override bool Enabled {
			get { 
				IEditor editor = HostWindow.Form.ActiveMdiChild as IEditor;
				return (editor != null && editor.FileName != null);
			}
		}

		public override void Execute() {
			IEditor editor = HostWindow.Form.ActiveMdiChild as IEditor;
			if (editor != null && editor.FileName != null)
				Clipboard.SetText(editor.FileName);
		}
	}
}