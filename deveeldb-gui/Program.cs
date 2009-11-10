using System;
using System.Windows.Forms;

using Deveel.Data.Plugins;
using Deveel.Data.Properties;

namespace Deveel.Data {
	static class Program {
		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static void Main() {
			Application.EnableVisualStyles();
			Application.SetCompatibleTextRenderingDefault(false);

			IApplicationServices services = ApplicationServices.Current;

			RegisterComponents(services);

			services.LoadPlugin(new CorePlugin());

			bool loadPlugins = (bool) services.Settings.GetProperty(SettingsProperties.LoadPlugins);

			if (loadPlugins) {
				IPlugin[] plugins = Plugin.FindPlugins(Environment.CurrentDirectory, Settings.Default.PluginFileFilter);
				Array.Sort(plugins, PluginComparer.Instance);
				foreach (IPlugin plugin in plugins) {
					services.LoadPlugin(plugin);
				}
			}

			Application.Run(services.HostWindow.Form);
		}

		private static void RegisterComponents(IApplicationServices services) {
			services.RegisterSingletonComponent("ApplicationSettings", typeof(ISettings), typeof(ApplicationSettings));
			services.RegisterSingletonComponent("HostWindow", typeof(IHostWindow), typeof(MainForm));
			services.RegisterSingletonComponent("FileEditorResolver", typeof(IFileEditorResolver), typeof(FileEditorResolver));

			services.RegisterComponent("QueryForm", typeof(IQueryEditor), typeof(QueryEditor));
		}
	}
}
