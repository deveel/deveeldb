using System;
using System.Collections;
using System.ComponentModel;
using System.Configuration;

namespace Deveel.Data.Configuration {
	public sealed class DotNetConfigurationFormatter : IConfigurationFormatter {
		public void LoadInto(IConfiguration config, IConfigurationSource source) {
			if (!(source is IDotNetConfigurationSource))
				throw new ArgumentException("The input source is invalid", "source");

			try {
				var settings = ((IDotNetConfigurationSource) source).Settings;

				foreach (SettingConfigurationElement setting in settings) {
					var key = setting.Name;
					var valueTypeName = setting.ValueType;
					var sourceValue = setting.Value;
					object value = sourceValue;

					if (!String.IsNullOrEmpty(sourceValue)) {
						if (!String.IsNullOrEmpty(valueTypeName)) {
							var valueType = Type.GetType(valueTypeName, true);
							if (valueType == null)
								throw new InvalidOperationException(String.Format("Value type '{0}' not found.", valueTypeName));

							var converter = TypeDescriptor.GetConverter(valueType);
							if (!converter.CanConvertFrom(typeof(string)))
								throw new InvalidOperationException(String.Format("Cannot convert from string to '{0}'.", valueType));

							value = converter.ConvertFromInvariantString(sourceValue);
						}

						config.SetValue(key, value);
					} else {
						config.SetValue(key, null);
					}
				}
			} catch (DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Unable to load the configurations form the source because of an error", ex);
			}
		}

		public void SaveFrom(IConfiguration config, ConfigurationLevel level, IConfigurationSource source) {
			if (!(source is IDotNetConfigurationSource))
				throw new ArgumentException("The input source is invalid", "source");

			var settings = new SettingConfigurationElementCollection(false);

			foreach (var pair in config) {
				var key = pair.Key;
				var value = pair.Value;
				var valueType = value != null ? value.GetType() : null;

				var setting = new SettingConfigurationElement {
					Name = key,
					Value = value == null ? "" : value.ToString(),
				};

				if (valueType != null)
					setting.ValueType = valueType.FullName;

				settings.Add(setting);
			}

			var filePath = new ExeConfigurationFileMap();
			if (source is DotNetConfigurationSectionSource) {
				filePath.ExeConfigFilename = ((DotNetConfigurationSectionSource) source).FilePath;
			} else if (source is DotNetDatabaseConfigurationSource) {
				filePath.ExeConfigFilename = ((DotNetDatabaseConfigurationSource) source).FilePath;
			}

			((IDotNetConfigurationSource) source).Settings = settings;

			var configuration = ConfigurationManager.OpenMappedExeConfiguration(filePath,
				ConfigurationUserLevel.PerUserRoamingAndLocal);

			configuration.Save(ConfigurationSaveMode.Modified);
		}
	}
}
