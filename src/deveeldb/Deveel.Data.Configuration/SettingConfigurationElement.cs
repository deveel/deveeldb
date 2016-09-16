using System;
using System.Configuration;

namespace Deveel.Data.Configuration {
	public sealed class SettingConfigurationElement : ConfigurationElement {
		[ConfigurationProperty("name", IsRequired = true, IsKey = true)]
		public string Name {
			get { return (string) this["name"]; }
			set { this["name"] = value; }
		}

		[ConfigurationProperty("value")]
		public string Value {
			get { return (string) this["value"]; }
			set { this["value"] = value; }
		}

		[ConfigurationProperty("valueType", DefaultValue = "System.String")]
		public string ValueType {
			get { return (string) this["valueType"]; }
			set { this["valueType"] = value; }
		}
	}
}
