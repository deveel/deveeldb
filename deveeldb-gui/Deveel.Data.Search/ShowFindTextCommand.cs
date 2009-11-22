using System;
using System.Windows.Forms;

using Deveel.Data.Commands;

namespace Deveel.Data.Search {
	[Command("ShowFindText", "&Find Text ...")]
	[CommandShortcut(Keys.Control | Keys.F, "Ctrl+F")]
	[CommandImage("Deveel.Data.Images.find.png")]
	public sealed class ShowFindTextCommand : Command {
		private IFindReplaceWindow window;

		public IFindReplaceWindow Window {
			get { return window; }
		}

		#region Overrides of Command

		public override bool Enabled {
			get { return HostWindow.ActiveChild is ITextSearchProvider; }
		}

		public override void Execute() {
			if (!Enabled) {
				return;
			}

			// if the window is an editor, grab the highlighted text
			ITextSearchProvider findReplaceProvider = HostWindow.ActiveChild as ITextSearchProvider;

			if (window == null || window.IsDisposed)
				window = new FindReplaceForm(Services);

			if (findReplaceProvider is IEditor)
				window.SearchText = ((IEditor)findReplaceProvider).SelectedText;
			
			window.TopMost = true;

			if (!window.Visible)
				window.Show(HostWindow.Form);
		}

		#endregion
	}
}