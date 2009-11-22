using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[Command("SaveFile", "&Save File")]
	[CommandShortcut(Keys.Control | Keys.S, "Ctrl+S")]
	[CommandImage("Deveel.Data.Images.disk.png")]
	public sealed class SaveFileCommand : Command {
		public override void Execute() {
			IApplicationServices services = ApplicationServices.Current;
			IEditor editor = HostWindow.Form.ActiveMdiChild as IEditor;
			if (editor != null) {
				if (editor.FileName == null) {
					services.CommandHandler.GetCommand(typeof(SaveFileAsCommand)).Execute();
				} else {
					editor.SaveFile();
				}
			}
		}

		public override bool Enabled {
			get {
				IEditor editor = HostWindow.Form.ActiveMdiChild as IEditor;
				return (editor != null ? editor.HasChanges : false);
			}
		}
	}
}