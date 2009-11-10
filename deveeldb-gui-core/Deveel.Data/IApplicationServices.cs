using System;
using System.Collections;

using Castle.MicroKernel;

using Deveel.Data.Commands;
using Deveel.Data.Plugins;

namespace Deveel.Data {
	public interface IApplicationServices {
		IKernel Container { get; }

		IHostWindow HostWindow { get; }

		ISettings Settings { get; }

		IDictionary Plugins { get; }

		CommandHandler CommandHandler { get; }

		Type[] ConfigurationTypes { get; }


		void LoadPlugin(IPlugin plugin);

		void InitPlugins();

		void RegisterConfiguration(Type configType);

		void RegisterSingletonComponent(string key, Type contractType, Type componentType);

		void RegisterComponent(string key, Type contractType, Type componentType);

		void RegisterComponent(string key, Type componentType);

		object Resolve(string key, Type type);

		object Resolve(Type type);

		void RegisterEditor(Type editorType, FileEditorInfo editorInfo);
	}
}