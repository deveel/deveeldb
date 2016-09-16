using System;
using System.Configuration;

namespace Deveel.Data.Configuration {
	public sealed class DotNetConfigurationSectionSource : IDotNetConfigurationSource {
		public DotNetConfigurationSectionSource(string sectionName)
			: this(null, sectionName) {
		}

		public DotNetConfigurationSectionSource(string filePath, string sectionName) {
			if (String.IsNullOrEmpty(sectionName))
				throw new ArgumentNullException("sectionName");

			FilePath = filePath;
			SectionName = sectionName;

			ConfigurationSection = DotNetSectionLoader.LoadConfiguration(filePath, sectionName);
		}


		public string SectionName { get; private set; }

		public string FilePath { get; private set; }

		public SystemConfigurationSection ConfigurationSection { get; private set; }

		SettingConfigurationElementCollection IDotNetConfigurationSource.Settings {
			get { return ConfigurationSection == null ? new SettingConfigurationElementCollection() : ConfigurationSection.Settings; }
			set { ConfigurationSection.Settings = value; }
		}
	}
}
