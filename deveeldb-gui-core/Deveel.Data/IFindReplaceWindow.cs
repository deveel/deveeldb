using System;
using System.Windows.Forms;

namespace Deveel.Data {
	public interface IFindReplaceWindow {
		string SearchText { get; set; }

		string ReplaceText { get; set; }

		bool TopMost { get; set; }

		bool Visible { get; set; }

		bool IsDisposed { get; }

		void Show(IWin32Window owner);
	}
}