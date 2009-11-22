using System;
using System.Collections;
using System.Windows.Forms;

using Castle.Core;
using Castle.MicroKernel;

using Deveel.Data.Commands;
using Deveel.Data.Plugins;

namespace Deveel.Data {
	public sealed class ApplicationServices : IApplicationServices {
		public ApplicationServices() {
			configTypes = new ArrayList();
			commandHandler = new CommandHandler(this);
		}

		static ApplicationServices() {
			container = new DefaultKernel();
			container.AddComponent("ApplicationServices", typeof(IApplicationServices), typeof(ApplicationServices),
			                       LifestyleType.Singleton);
		}

		private static readonly IKernel container;
		private readonly Hashtable plugins = new Hashtable();
		private readonly ArrayList configTypes;
		private readonly CommandHandler commandHandler;

		public static ApplicationServices Current {
			get { return (ApplicationServices) container.GetService(typeof (IApplicationServices)); }
		}

		public CommandHandler CommandHandler {
			get { return commandHandler; }
		}

		public Type[] ConfigurationTypes {
			get { return (Type[]) configTypes.ToArray(typeof (Type)); }
		}

		public IKernel Container {
			get { return container; }
		}

		public IHostWindow HostWindow {
			get { return (IHostWindow) container.GetService(typeof (IHostWindow)); }
		}

		public ISettings Settings {
			get { return (ISettings) container.GetService(typeof (ISettings)); }
		}

		public IDictionary Plugins {
			get { return (IDictionary) plugins.Clone(); }
		}

		public void LoadPlugin(IPlugin plugin) {
			if (plugin == null)
				throw new ArgumentNullException("plugin");

			plugin.Load(this);

			Type pluginType = plugin.GetType();

			plugins[pluginType] = plugin;
			container.AddComponent(pluginType.FullName, typeof(IPlugin), pluginType);
		}

		public void InitPlugins() {
			if (plugins == null || plugins.Count == 0)
				return;

			ArrayList pluginList = new ArrayList(plugins.Values);
			pluginList.Sort(PluginComparer.Instance);

			foreach (IPlugin plugin in pluginList) {
				IHostWindow hostWindow = HostWindow;

				try {
					if (hostWindow != null)
						hostWindow.SetStatus(null, "Initializing " + plugin.Name);

					plugin.Init();
				} catch (Exception e) {
					if (hostWindow == null)
						throw;

					hostWindow.DisplayMessageBox(
						null,
						string.Format("Error Initializing {0}:{1}{2}", plugin.Name, Environment.NewLine, e),
						"Plugin Error",
						MessageBoxButtons.OK,
						MessageBoxIcon.Warning,
						MessageBoxDefaultButton.Button1,
						0,
						null,
						null);
				}
			}
		}

		public void RegisterSingletonComponent(string key, Type contractType, Type componentType) {
			container.AddComponent(key, contractType, componentType, LifestyleType.Singleton);
		}

		public void RegisterComponent(string key, Type contractType, Type serviceType) {
			container.AddComponent(key, contractType, serviceType, LifestyleType.Transient);
		}

		public void RegisterComponent(string key, Type serviceType) {
			container.AddComponent(key, serviceType, LifestyleType.Transient);
		}

		public void RegisterConfiguration(Type configType) {
			if (!typeof(IConfiguration).IsAssignableFrom(configType))
				throw new ArgumentException();

			RegisterComponent(configType.FullName, typeof(IConfiguration), configType);
			configTypes.Add(configType);
		}

		public object Resolve(string key, Type type) {
			if (key == null || key.Length == 0)
				return Resolve(type);
			return container.Resolve(key, type);
		}

		public object Resolve(Type type) {
			return container.Resolve(type);
		}

		public void RegisterEditor(Type editorType, FileEditorInfo editorInfo) {
			if (!typeof(IEditor).IsAssignableFrom(editorType))
				throw new ArgumentException();

			RegisterComponent(editorInfo.Key, typeof(IEditor), editorType);

			// push the ext reg into the resolver....
			IFileEditorResolver resolver = (IFileEditorResolver) Resolve(typeof(IFileEditorResolver));
			resolver.Register(editorInfo);
		}
	}
}