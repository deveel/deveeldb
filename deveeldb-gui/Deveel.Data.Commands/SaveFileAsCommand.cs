using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("SaveFileAs", "Save File &As...")]
	[CommandShortcut(Keys.Control | Keys.Alt | Keys.S, "Ctrl+Alt+S")]
	[CommandImage("Deveel.Data.Images.disk_multiple.png")]
	public sealed class SaveFileAsCommand : Command {
		public override bool Enabled {
			get {
				IEditor editor = HostWindow.Form.ActiveMdiChild as IEditor;
				return (editor != null ? editor.HasChanges : false);
			}
		}

		public override void Execute() {
			IEditor editor = HostWindow.Form.ActiveMdiChild as IEditor;
			if (editor != null) {
				SaveFileDialog saveFileDialog = new SaveFileDialog();
				saveFileDialog.InitialDirectory = Environment.GetFolderPath(Environment.SpecialFolder.MyComputer);
				saveFileDialog.Filter = editor.FileFilter;
				if (saveFileDialog.ShowDialog(HostWindow.Form) == DialogResult.OK) {
					editor.FileName = saveFileDialog.FileName;
					editor.SaveFile();
				}
			}
		}
	}
}