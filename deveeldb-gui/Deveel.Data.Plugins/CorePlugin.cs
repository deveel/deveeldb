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

			services.RegisterEditor(typeof(Editor), new FileEditorInfo("default", "Text editor"));
			services.RegisterEditor(typeof(Editor), new FileEditorInfo("txt-editor", "Text Editor", "txt"));
			services.RegisterEditor(typeof(QueryEditor), new FileEditorInfo("sql-editor", "SQL Editor", "sql"));

			services.RegisterComponent("NewFileForm", typeof(NewFileForm));
			services.RegisterComponent("OptionsForm", typeof(OptionsForm));

			services.RegisterConfiguration(typeof (CoreConfiguration));

			ToolStripMenuItem fileMenu = hostWindow.GetMenuItem("File");
			ToolStripMenuItem editMenu = hostWindow.GetMenuItem("Edit");
			ToolStripMenuItem queryMenu = hostWindow.GetMenuItem("Query");

			// File
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

			// Edit
			editMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(UndoCommand)));
			editMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(RedoCommand)));
			editMenu.DropDownItems.Add(new ToolStripSeparator());
			editMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(CutTextCommand)));
			editMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(CopyTextCommand)));
			editMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(PasteTextCommand)));
			editMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(ToLowerCaseCommand)));
			editMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(ToUpperCaseCommand)));
			editMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(ToTitleCaseCommand)));
			editMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(DeleteTextCommand)));
			editMenu.DropDownItems.Add(new ToolStripSeparator());
			editMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(ShowOptionsCommand)));

			// Query
			queryMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(ExecuteTaskCommand)));
			queryMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(CancelTaskCommand)));
			queryMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(ResetConnectionCommand)));
			queryMenu.DropDownItems.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(CloseConnectionCommand)));


			CommandControlBuilder.MonitorMenuItemsOpeningForEnabling(hostWindow.Form.MainMenuStrip);

			hostWindow.AddToolStripCommand(0, typeof(NewQueryFormCommand));
			hostWindow.AddToolStripCommand(1, typeof(OpenFileCommand));
			hostWindow.AddToolStripCommand(2, typeof(SaveFileCommand));
			hostWindow.AddToolStripCommand(3, typeof(SaveFileAsCommand));
			hostWindow.AddToolStripSeperator(4);
			hostWindow.AddToolStripCommand(5, typeof(ExecuteTaskCommand));
			hostWindow.AddToolStripCommand(6, typeof(CancelTaskCommand));
			hostWindow.AddToolStripSeperator(7);
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