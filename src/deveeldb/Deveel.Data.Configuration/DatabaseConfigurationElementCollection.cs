using System;
using System.Configuration;

namespace Deveel.Data.Configuration {
	public sealed class DatabaseConfigurationElementCollection : ConfigurationElementCollection {
		protected override ConfigurationElement CreateNewElement() {
			return new DatabaseConfigurationElement();
		}

		protected override object GetElementKey(ConfigurationElement element) {
			return ((DatabaseConfigurationElement) element).Name;
		}
	}
}
