using System;
using System.Collections;
using System.Windows.Forms;

using Deveel.Data.Commands;
using Deveel.Data.DbModel;
using Deveel.Data.Plugins;
using Deveel.Data.Properties;

using WeifenLuo.WinFormsUI.Docking;

namespace Deveel.Data {
	public partial class MainForm : Form, IHostWindow {
		public MainForm() {
			InitializeComponent();
			SetPointerState(Cursors.AppStarting);
		}

		public MainForm(IApplicationServices services)
			: this() {
			services.Settings.ConnectionStringsChanged += new EventHandler(Settings_ConnectionStringsChanged);
			this.services = services;
			commandControlBuilder = new CommandControlBuilder(services.CommandHandler);
		}

		private readonly IApplicationServices services;
		private Form activeChild;
		private readonly CommandControlBuilder commandControlBuilder;
		private IDbMetadataProvider metadataProvider;
		private bool initd;

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

		public void ShowMetadata(IDbMetadataProvider provider, DockState dockState) {
			if (metadataProvider != null && metadataProvider != provider) {
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
			e.Effect = e.Data.GetDataPresent(DataFormats.FileDrop) ? DragDropEffects.Copy : DragDropEffects.None;
		}

		private void MainForm_DragDrop(object sender, DragEventArgs e) {
			if (e.Data.GetDataPresent(DataFormats.FileDrop)) {
				string[] filePaths = (string[])(e.Data.GetData(DataFormats.FileDrop));
				IFileEditorResolver resolver = (IFileEditorResolver)services.Resolve(typeof(IFileEditorResolver));
				foreach (string filename in filePaths) {
					IEditor editor = resolver.ResolveEditor(filename);
					editor.FileName = filename;
					editor.LoadFile();
					DisplayDockedForm(editor as DockContent);
				}
			}
		}

		private void MainForm_Load(object sender, EventArgs e) {
			services.Settings.ConnectionStringsChanged += new EventHandler(Settings_ConnectionStringsChanged);
		}

		private void Settings_ConnectionStringsChanged(object sender, EventArgs e) {
			bool load = true;

			if (initd) {

				DialogResult result = MessageBox.Show(this,
													  "The connections have changed, would you like to refresh the database connection?",
													  "Reload Connection?",
													  MessageBoxButtons.YesNo, MessageBoxIcon.Question,
													  MessageBoxDefaultButton.Button1);
				if (result != DialogResult.Yes)
					load = false;
			}

			if (load)
				LoadConnections();
		}

		private void LoadConnections() {
			DbConnectionStrings connStrings = services.Settings.ConnectionStrings;

			toolStripComboBoxConnection.Items.Clear();
			foreach (DbConnectionString connString in connStrings.Strings) {
				toolStripComboBoxConnection.Items.Add(connString);
				if (connString.Name == Settings.Default.NammedConnection) {
					toolStripComboBoxConnection.SelectedItem = connString;
					((ApplicationSettings)services.Settings).ConnectionString = connString;
					SetWindowTitle(connString.Name);
				}
			}
		}

		private void SetWindowTitle(string connectionName) {
			Text = string.Format("DeveelDB Client [{0}]", connectionName);
		}

		private void MainForm_Shown(object sender, EventArgs e) {
			services.InitPlugins();

			DockContent provider = metadataProvider as DockContent;
			if (provider != null)
				provider.Activate();

			initd = true;

			SetPointerState(Cursors.Default);
			SetStatus(null, string.Empty);

			// services.CommandHandler.GetCommand(typeof(NewQueryFormCommand)).Execute();
		}

		private void MainForm_FormClosing(object sender, FormClosingEventArgs e) {
			if (e.Cancel) {
				return;
			}

			if (services.Settings.ConnectionString != null) {
				Settings.Default.NammedConnection = services.Settings.ConnectionString.Name;
				Settings.Default.Save();
			}

			ArrayList plugins = new ArrayList(services.Plugins.Values);
			plugins.Reverse();
			foreach (IPlugin plugin in plugins)
				plugin.Unload();

			services.Container.Dispose();
		}

		private void MainForm_MdiChildActivate(object sender, EventArgs e) {
			activeChild = ActiveMdiChild;
		}

		private void hideShowToolStripMenuItem_Click(object sender, EventArgs e) {
			if (WindowState == FormWindowState.Normal) {
				Hide();
				hideShowToolStripMenuItem.Text = "Show";
				WindowState = FormWindowState.Minimized;
			} else {
				Show();
				hideShowToolStripMenuItem.Text = "Hide";
				WindowState = FormWindowState.Normal;
			}
		}

		private void exitToolStripMenuItem_Click(object sender, EventArgs e) {
			services.CommandHandler.GetCommand(typeof(ExitCommand)).Execute();
		}

		private void toolStripLower_DoubleClick(object sender, EventArgs e) {
			if (WindowState == FormWindowState.Normal) {
				Hide();
				hideShowToolStripMenuItem.Text = "Show";
				WindowState = FormWindowState.Minimized;
			} else {
				Show();
				hideShowToolStripMenuItem.Text = "Hide";
				WindowState = FormWindowState.Normal;
			}
		}
	}
}
