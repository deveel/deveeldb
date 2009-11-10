using System;
using System.Windows.Forms;

using Deveel.Data.Commands;

using WeifenLuo.WinFormsUI.Docking;

namespace Deveel.Data {
	public interface IHostWindow {
		Form Form { get; }

		Form ActiveChild { get; }

		IDbMetadataProvider MetadataProvider { get; }

		ToolStrip ToolStrip { get; }


		void SetStatus(Form source, string text);

		DialogResult DisplayMessageBox(Form source, string text, string caption, MessageBoxButtons buttons,
		                               MessageBoxIcon icon, MessageBoxDefaultButton defaultButton, MessageBoxOptions options,
		                               string helpFilePath, string keyword);

		DialogResult DisplaySimpleMessageBox(Form source, string text, string caption);

		void SetPointerState(Cursor cursor);

		void DisplayDockedForm(DockContent form);

		void ShowToolWindow(DockContent form, DockState dockState);

		ToolStripMenuItem GetMenuItem(string name);

		void AddPluginCommand(Type commandType);

		void AddToolStripCommand(int index, Type commandType);

		void AddToolStripSeperator(int index);

		void ShowDatabaseInspector(IDbMetadataProvider metadataProvider, DockState dockState);
	}
}