using System;
using System.Configuration;

namespace Deveel.Data.Configuration {
	public sealed class DatabaseConfigurationElement : ConfigurationElement {
		[ConfigurationProperty("name", IsRequired = true, IsKey = true)]
		public string Name {
			get { return (string) this["name"]; }
			set { this["name"] = value; }
		}

		[ConfigurationProperty("settings")]
		[ConfigurationCollection(typeof(SettingConfigurationElement))]
		public SettingConfigurationElementCollection Settings {
			get { return (SettingConfigurationElementCollection) this["settings"]; }
			set { this["settings"] = value; }
		}
	}
}
