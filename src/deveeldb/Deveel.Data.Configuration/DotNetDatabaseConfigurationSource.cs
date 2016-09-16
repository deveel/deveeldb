using System;
using System.Collections;
using System.Linq;

namespace Deveel.Data.Configuration {
	public sealed class DotNetDatabaseConfigurationSource : IDotNetConfigurationSource {
		private readonly SystemConfigurationSection section;

		public DotNetDatabaseConfigurationSource(string filePath, string sectionName, string databaseName) {
			if (String.IsNullOrEmpty(sectionName))
				throw new ArgumentNullException("sectionName");
			if (String.IsNullOrEmpty(databaseName))
				throw new ArgumentOutOfRangeException("databaseName");

			FilePath = filePath;
			SectionName = sectionName;
			DatabaseName = databaseName;

			section  = DotNetSectionLoader.LoadConfiguration(filePath, sectionName);

			DatabaseConfigurationElement element = null;
			foreach (DatabaseConfigurationElement database in section.Databases) {
				if (database.Name == databaseName) {
					element = database;
					break;
				}
			}

			ConfigurationElement = element;
		}

		public string SectionName { get; private set; }

		public string FilePath { get; private set; }

		public string DatabaseName { get; private set; }

		public DatabaseConfigurationElement ConfigurationElement { get; private set; }

		SystemConfigurationSection IDotNetConfigurationSource.ConfigurationSection {
			get { return section; }
		}

		SettingConfigurationElementCollection IDotNetConfigurationSource.Settings {
			get { return ConfigurationElement == null ? new SettingConfigurationElementCollection() : ConfigurationElement.Settings; }
			set { ConfigurationElement.Settings = value; }
		}
	}
}
