using System;

namespace Deveel.Data.Configuration {
	interface IDotNetConfigurationSource : IConfigurationSource {
		string SectionName { get; }

		SystemConfigurationSection ConfigurationSection { get; }

		SettingConfigurationElementCollection Settings { get; set; }
	}
}
