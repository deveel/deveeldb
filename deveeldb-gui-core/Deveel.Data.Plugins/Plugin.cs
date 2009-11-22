using System;
using System.Collections;
using System.IO;
using System.Reflection;

namespace Deveel.Data.Plugins {
	public abstract class Plugin : IPlugin {
		protected Plugin(string name, string description, int loadOrder) {
			this.name = name;
			this.description = description;
			this.loadOrder = loadOrder;

			RetrievePluginAttributes();
		}

		protected Plugin(string name, string description)
			: this(name, description, 1000) {
		}

		protected Plugin(string name)
			: this(name, null) {
		}

		protected Plugin()
			: this(null) {
		}

		private IApplicationServices services;
		private string name;
		private string description;
		private int loadOrder;

		public int Order {
			get { return loadOrder; }
		}

		public string Name {
			get { return name; }
		}

		public string Description {
			get { return description; }
		}

		protected IApplicationServices Services {
			get { return services; }
		}

		private void RetrievePluginAttributes() {
			PluginAttribute attribute = Attribute.GetCustomAttribute(GetType(), typeof(PluginAttribute)) as PluginAttribute;
			if (attribute == null)
				return;

			name = attribute.Name;
			loadOrder = attribute.Order;
			description = attribute.Description;
		}

		public virtual void Load(IApplicationServices context) {
			services = context;
		}

		public abstract void Init();

		public virtual void Unload() {
		}

		public static IPlugin[] FindPlugins(string baseDir, string searchPattern) {
			ArrayList plugins = new ArrayList();

			try {
				string[] files = Directory.GetFiles(baseDir, searchPattern);

				foreach (string file in files) {
					Assembly pluginAssembly = Assembly.LoadFrom(file);
					Type[] assemblyTypes = pluginAssembly.GetTypes();

					foreach (Type assemblyType in assemblyTypes) {
						if (typeof(IPlugin).IsAssignableFrom(assemblyType) &&
							!assemblyType.IsAbstract)
							plugins.Add(Activator.CreateInstance(assemblyType));
					}
				}
			} catch (TargetInvocationException e) {
				if (e.InnerException != null) {
					throw e.InnerException;
				}
			}

			return (IPlugin[]) plugins.ToArray(typeof(IPlugin));
		}

	}
}