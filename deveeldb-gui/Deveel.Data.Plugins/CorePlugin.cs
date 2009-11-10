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

			services.RegisterEditor(typeof(Editor), new FileEditorInfo("Text editor", "default"));
			services.RegisterEditor(typeof(Editor), new FileEditorInfo("Text Editor", "txt-editor", "txt"));
			//TODO: register the SQL editor

			services.RegisterComponent("NewFileForm", typeof(NewFileForm));
			services.RegisterComponent("OptionsForm", typeof(OptionsForm));

			//TODO: register the configuration handler...

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