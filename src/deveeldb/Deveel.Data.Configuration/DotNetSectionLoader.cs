using System;
using System.Configuration;

namespace Deveel.Data.Configuration {
	static class DotNetSectionLoader {
		public static SystemConfigurationSection LoadConfiguration(string filePath, string sectionName) {
			try {
				var exeFilePath = new ExeConfigurationFileMap();
				if (!String.IsNullOrEmpty(filePath)) {
					exeFilePath.ExeConfigFilename = filePath;
				}

				var configuration = ConfigurationManager.OpenMappedExeConfiguration(exeFilePath,
					ConfigurationUserLevel.PerUserRoaming);

				var section = configuration.GetSection(sectionName);
				if (section == null)
					throw new DatabaseConfigurationException(
						String.Format("Could not find the section '{0}' in configuration file '{1}'.", sectionName, configuration.FilePath));

				if (!(section is SystemConfigurationSection))
					throw new DatabaseConfigurationException(String.Format(
						"The section '{0}' in configuration file '{1}' is invalid.", sectionName, configuration.FilePath));

				return (SystemConfigurationSection)section;
			} catch (DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Unable to load the configuration because of an error.", ex);
			}
		}
	}
}
