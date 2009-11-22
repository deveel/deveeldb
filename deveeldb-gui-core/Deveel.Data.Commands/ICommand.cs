using System;
using System.Drawing;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	public interface ICommand {
		string Name { get; }

		string Text { get; }

		Keys Shortcut { get; }

		string ShortcutText { get; }

		Image SmallImage { get; }

		bool Enabled { get; }


		void Execute();
	}
}