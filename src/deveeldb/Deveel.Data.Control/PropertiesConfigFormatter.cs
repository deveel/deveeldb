using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

using Deveel.Data.Util;

namespace Deveel.Data.Control {
	public sealed class PropertiesConfigFormatter : IConfigFormatter {
		public void LoadFrom(DbConfig config, Stream inputStream) {
			Properties properties = new Properties();
			properties.Load(inputStream);

			foreach (DictionaryEntry property in properties) {
				config.SetValue((string)property.Key, (string) property.Value);
			}
		}

		public void SaveTo(DbConfig config, Stream outputStream) {
			Properties properties = new Properties();
			foreach (KeyValuePair<string, object> pair in config) {
				string value = Convert.ToString(pair.Value, CultureInfo.InvariantCulture);
				properties.SetProperty(pair.Key, value);
			}

			properties.Store(outputStream, null);
		}
	}
}