using System;

namespace Deveel.Data.Plugins {
	public interface IPlugin {
		int Order { get; }

		string Name { get; }

		string Description { get; }


		void Load(IApplicationServices context);

		void Init();

		void Unload();
	}
}