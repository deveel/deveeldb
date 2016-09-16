using System;
using System.Configuration;

namespace Deveel.Data.Configuration {
	public sealed class SettingConfigurationElementCollection : ConfigurationElementCollection {
		private bool? isReadOnly;

		public SettingConfigurationElementCollection() {
		}

		internal SettingConfigurationElementCollection(bool readOnly) {
			isReadOnly = readOnly;
		}
		protected override ConfigurationElement CreateNewElement() {
			return new SettingConfigurationElement();
		}

		protected override object GetElementKey(ConfigurationElement element) {
			return ((SettingConfigurationElement) element).Name;
		}

		internal void Add(SettingConfigurationElement element) {
			BaseAdd(element, true);
		}

		public override bool IsReadOnly() {
			if (isReadOnly == null)
				return base.IsReadOnly();

			return isReadOnly.Value;
		}
	}
}
