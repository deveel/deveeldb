using System;
using System.Windows.Forms;

using Deveel.Data.Commands;

namespace Deveel.Data.Plugins {
	public sealed class CorePlugin : Plugin {
		public CorePlugin()
			: base("Core Plugin", "Sets up the core functionalities of the client.", 1) {
		}

		private Timer timer;

		#region Overrides of Plugin

		public override void Init() {
			IApplicationServices services = Services;
			IHostWindow hostWindow = services.HostWindow;

			CommandControlBuilder commandControlBuilder = new CommandControlBuilder(services.CommandHandler);

			services.RegisterEditor(typeof(Editor), new FileEditorInfo("Text editor", "default"));
			services.RegisterEditor(typeof(Editor), new FileEditorInfo("Text Editor", "txt-editor", "txt"));
			services.RegisterEditor(typeof(QueryEditor), new FileEditorInfo("SQL Editor", "sql-editor", "sql"));

			services.RegisterComponent("NewFileForm", typeof(NewFileForm));
			services.RegisterComponent("OptionsForm", typeof(OptionsForm));

			services.RegisterConfiguration(typeof (CoreConfiguration));

			ToolStripMenuItem fileMenu = hostWindow.GetMenuItem("File");
			ToolStripMenuItem editMenu = hostWindow.GetMenuItem("Edit");
			ToolStripMenuItem queryMenu = hostWindow.GetMenuItem("Query");

			fileMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(NewQueryFormCommand)));
			fileMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(NewFileCommand)));
			fileMenu.DropDownItems.Add(new ToolStripSeparator());
			fileMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(OpenFileCommand)));
			fileMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(SaveFileCommand)));
			fileMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(SaveFileAsCommand)));
			fileMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(CloseActiveChildCommand)));
			fileMenu.DropDownItems.Add(new ToolStripSeparator());
			fileMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(PrintCommand)));
			fileMenu.DropDownItems.Add(new ToolStripSeparator());
			fileMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(ExitCommand)));

			queryMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(ExecuteTaskCommand)));
			queryMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(CancelTaskCommand)));
			queryMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(ResetConnectionCommand)));
			queryMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(CloseConnectionCommand)));

			CommandControlBuilder.MonitorMenuItemsOpeningForEnabling(hostWindow.Form.MainMenuStrip);

			hostWindow.AddToolStripCommand(0, typeof(NewQueryFormCommand));
			hostWindow.AddToolStripCommand(1, typeof(OpenFileCommand));
			hostWindow.AddToolStripCommand(2, typeof(SaveFileCommand));
			hostWindow.AddToolStripSeperator(3);
			hostWindow.AddToolStripCommand(4, typeof(ExecuteTaskCommand));
			hostWindow.AddToolStripCommand(5, typeof(CancelTaskCommand));
			hostWindow.AddToolStripSeperator(6);
			hostWindow.AddToolStripSeperator(-1);
			hostWindow.AddToolStripCommand(-1, typeof(ResetConnectionCommand));

			timer = new Timer();
			timer.Interval = 1000;
			timer.Tick += new EventHandler(timer_Tick);
			timer.Enabled = true;
		}

		public override void Unload() {
			if (timer != null)
				timer.Dispose();
		}

		#endregion

		void timer_Tick(object sender, EventArgs e) {
			try {
				timer.Enabled = false;
				foreach (ToolStripItem item in Services.HostWindow.ToolStrip.Items) {
					ICommand cmd = item.Tag as ICommand;
					if (cmd != null)
						item.Enabled = cmd.Enabled;
				}
			} finally {
				timer.Enabled = true;
			}
		}
	}
}