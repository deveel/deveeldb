using System;
using System.Windows.Forms;

using Deveel.Data.Commands;

using WeifenLuo.WinFormsUI.Docking;

namespace Deveel.Data.Deveel.Data {
	public partial class MainForm : Form, IHostWindow {
		public MainForm(IApplicationServices services) {
			InitializeComponent();
			this.services = services;
			commandControlBuilder = new CommandControlBuilder(services.CommandHandler);
		}

		private readonly IApplicationServices services;
		private Form activeChild;
		private readonly CommandControlBuilder commandControlBuilder;
		private IDbMetadataProvider metadataProvider;

		public Form Form {
			get { return this; }
		}

		public Form ActiveChild {
			get { return activeChild; }
		}

		public IDbMetadataProvider MetadataProvider {
			get { return ((metadataProvider == null || ((Form) metadataProvider).IsDisposed) ? null : metadataProvider); }
		}

		public ToolStrip ToolStrip {
			get { return toolStripLower; }
		}

		internal void SetActiveChild(Form form) {
			activeChild = form;
		}

		public void SetStatus(Form source, string text) {
			if (source == null || ActiveMdiChild == source) {
				if (text != null)
					text = text.Trim();

				toolStripStatusLabel.Text = text;
			}
		}

		public DialogResult DisplayMessageBox(Form source, string text, string caption,
			MessageBoxButtons buttons, MessageBoxIcon icon, MessageBoxDefaultButton defaultButton, MessageBoxOptions options,
			string helpFilePath, string keyword) {
			if (helpFilePath == null && keyword == null)
				return MessageBox.Show(source, text, caption, buttons, icon, defaultButton, options);
			return MessageBox.Show(source, text, caption, buttons, icon, defaultButton, options, helpFilePath, keyword);

		}

		public DialogResult DisplaySimpleMessageBox(Form source, string text, string caption) {
			return MessageBox.Show(source, text, caption);
		}

		public void SetPointerState(Cursor cursor) {
			Cursor = cursor;
			Application.DoEvents();
		}

		public void DisplayDockedForm(DockContent form) {
			if (form != null)
				form.Show(dockPanel, DockState.Document);
		}

		public void ShowToolWindow(DockContent form, DockState dockState) {
			form.Show(dockPanel, dockState);
		}

		public ToolStripMenuItem GetMenuItem(string name) {
			foreach (ToolStripMenuItem item in MainMenuStrip.Items) {
				string itemName = item.Text.Replace("&", String.Empty);
				if (String.Compare(itemName, name, true) == 0)
					return item;
			}

			return null;
		}

		public void ShowDatabaseInspector(IDbMetadataProvider provider, DockState dockState) {
			if (provider != null && provider != metadataProvider) {
				metadataProvider.Close();
			}

			metadataProvider = provider;
			DockContent form = metadataProvider as DockContent;
			if (form == null)
				throw new InvalidOperationException();

			form.Show(dockPanel, dockState);
		}

		public void AddPluginCommand(Type commandType) {
			pluginsToolStripMenuItem.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(commandType));
		}

		public void AddToolStripCommand(int index, Type commandType) {
			ToolStripButton item = commandControlBuilder.CreateToolStripButton(commandType);
			if (index == -1) {
				toolStripLower.Items.Add(item);
			} else {
				toolStripLower.Items.Insert(index, item);
			}
		}

		public void AddToolStripSeperator(int index) {
			ToolStripSeparator item = new ToolStripSeparator();
			if (index == -1) {
				toolStripLower.Items.Add(item);
			} else {
				toolStripLower.Items.Insert(index, item);
			}
		}

		private void MainForm_DragEnter(object sender, DragEventArgs e) {

		}

		private void MainForm_DragDrop(object sender, DragEventArgs e) {

		}

		private void MainForm_Load(object sender, EventArgs e) {
			services.Settings.ConnectionStringsChanged += new EventHandler(Settings_ConnectionStringsChanged);
		}

		void Settings_ConnectionStringsChanged(object sender, EventArgs e) {
			throw new NotImplementedException();
		}

		private void MainForm_Shown(object sender, EventArgs e) {
			services.InitPlugins();
		}

		private void MainForm_FormClosing(object sender, FormClosingEventArgs e) {
			if (e.Cancel)
				return;


		}

		private void MainForm_MdiChildActivate(object sender, EventArgs e) {
			activeChild = ActiveMdiChild;
		}
	}
}
