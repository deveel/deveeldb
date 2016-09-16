using System;
using System.Configuration;

namespace Deveel.Data.Configuration {
	public sealed class SystemConfigurationSection : ConfigurationSection {
		[ConfigurationProperty("databases")]
		[ConfigurationCollection(typeof(DatabaseConfigurationElement))]
		public DatabaseConfigurationElementCollection Databases {
			get { return (DatabaseConfigurationElementCollection) this["databases"]; }
			set { this["databases"] = value; }
		}

		[ConfigurationProperty("settings")]
		[ConfigurationCollection(typeof(SettingConfigurationElement))]
		public SettingConfigurationElementCollection Settings {
			get { return (SettingConfigurationElementCollection) this["settings"]; }
			set { this["settings"] = value; }
		}
	}
}
