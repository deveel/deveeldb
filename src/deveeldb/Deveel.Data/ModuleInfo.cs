using System;

namespace Deveel.Data {
	public sealed class ModuleInfo {
		public ModuleInfo(string moduleName, string version) {
			ModuleName = moduleName;
			Version = version;
		}

		public string ModuleName { get; private set; }

		public string Version { get; private set; }
	}
}
