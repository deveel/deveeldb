using System;

namespace Deveel.Data.Configuration {
	public class ConfigurationBuilder : IConfigurationBuilder {
		private Configuration configuration;

		public ConfigurationBuilder() {
			configuration = new Configuration();
		}

		public IConfigurationBuilder WithSetting(string key, object value) {
			configuration.SetValue(key, value);
			return this;
		}

		public IConfiguration Build() {
			return configuration;
		}
	}
}
