using System;
using System.Windows.Forms;

using WeifenLuo.WinFormsUI.Docking;

namespace Deveel.Data.Commands {
	[Command("NewFile", "New &File")]
	[CommandShortcut(Keys.Control | Keys.Alt | Keys.N, "Ctrl+Alt+N")]
	[CommandImage("Deveel.Data.Images.page.png")]
	public sealed class NewFileCommand : Command {
		public override void Execute() {
			NewFileForm newFileForm = (NewFileForm) Services.Resolve(typeof(NewFileForm));

			DialogResult result = newFileForm.ShowDialog();

			if (result == DialogResult.OK) {
				IEditor editor = (IEditor) Services.Resolve(newFileForm.FileEditorInfo.Key, typeof(IEditor));
				editor.FileName = null;
				HostWindow.DisplayDockedForm(editor as DockContent);
			}
		}
	}
}