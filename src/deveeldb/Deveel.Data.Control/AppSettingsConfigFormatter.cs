using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.IO;

namespace Deveel.Data.Control {
	public sealed class AppSettingsConfigFormatter : IConfigFormatter {
		public void Load(DbConfig config) {
			NameValueCollection settings = ConfigurationSettings.AppSettings;
			foreach (string key in settings) {
				string value = settings.Get(key);
				config.SetValue(key, value);
			}			
		}

		void IConfigFormatter.LoadFrom(DbConfig config, Stream inputStream) {
			Load(config);
		}

		void IConfigFormatter.SaveTo(DbConfig config, Stream outputStream) {
			Save(config);
		}

		public void Save(DbConfig config) {
			foreach (KeyValuePair<string, object> pair in config) {
				string value = Convert.ToString(pair.Value);
				ConfigurationSettings.AppSettings.Set(pair.Key, value);
			}
		}
	}
}