using System;
using System.Windows.Forms;

using WeifenLuo.WinFormsUI.Docking;

namespace Deveel.Data.Commands {
	[CommandSmallImage("Deveel.Data.Images.page_white.png")]
	public sealed class NewQueryFormCommand : Command {
		public NewQueryFormCommand()
			: base("New &Query Window", Keys.Control | Keys.N, "Ctrl+N") {
		}

		public override void Execute() {
			IQueryEditor editor = (IQueryEditor) Services.Container.Resolve(typeof(IQueryEditor));
			editor.FileName = null;
			HostWindow.DisplayDockedForm(editor as DockContent);
		}
	}
}