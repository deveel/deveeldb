// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

using DryIoc;

namespace Deveel.Data.Configuration {
	public static class ConfigurationExtensions {
		#region Get Values

		public static IEnumerable<string> GetKeys(this IConfiguration config) {
			return config.GetKeys(ConfigurationLevel.Current);
		}

		public static IEnumerable<object> GetValues(this IConfiguration config, ConfigurationLevel level) {
			var keys = config.GetKeys(level);
			var values = keys.Select(x => config.GetValue(x))
				.Where(value => value != null)
				.ToList();

			return values.ToArray();
		}

		#region GetValue(string)

		public static object GetValue(this IConfiguration config, string keyName) {
			return GetValue(config, keyName, null);
		}

		public static object GetValue(this IConfiguration config, string keyName, object defaultValue) {
			var value = config.GetValue(keyName);
			if (value == null)
				return defaultValue;

			return value;
		}

		public static T GetValue<T>(this IConfiguration config, string keyName) {
			return GetValue<T>(config, keyName, default(T));
		}

		private static T ToType<T>(object value) {
			if (value == null)
				return default(T);

			if (value is T)
				return (T)value;

			if (typeof(T) == typeof(bool) &&
				value is string)
				return (T)ConvertToBoolean((string)value);

			if (typeof(T).IsEnum())
				return ConvertToEnum<T>(value);

#if !PCL
			if (!(value is IConvertible))
				throw new InvalidCastException();
#endif

			return (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
		}

		private static T ConvertToEnum<T>(object value) {
			if (value is int ||
				value is short ||
				value is long ||
				value is byte)
				return (T)value;

			if (value == null)
				return default(T);

			var s = value.ToString();
			return (T)Enum.Parse(typeof(T), s, true);
		}

		private static object ConvertToBoolean(string value) {
			if (String.Equals(value, "true", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(value, "enabled", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(value, "1") ||
				String.Equals(value, "on", StringComparison.OrdinalIgnoreCase))
				return true;
			if (String.Equals(value, "false", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(value, "disabled", StringComparison.OrdinalIgnoreCase) ||
				String.Equals(value, "0") ||
				String.Equals(value, "off"))
				return false;

			throw new InvalidCastException();
		}

		public static T GetValue<T>(this IConfiguration config, string keyName, T defaultValue) {
			var value = config.GetValue(keyName);
			if (value == null)
				return defaultValue;

			return ToType<T>(value);
		}

		public static string GetString(this IConfiguration config, string propertyKey) {
			return GetString(config, propertyKey, null);
		}

		public static string GetString(this IConfiguration config, string propertyKey, string defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static byte GetByte(this IConfiguration config, string propertyKey) {
			return GetByte(config, propertyKey, 0);
		}

		public static byte GetByte(this IConfiguration config, string propertyKey, byte defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		[CLSCompliant(false)]
		public static sbyte GetSByte(this IConfiguration config, string propertyKey, sbyte defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static short GetInt16(this IConfiguration config, string propertyKey) {
			return GetInt16(config, propertyKey, 0);
		}

		public static short GetInt16(this IConfiguration config, string propertyKey, short defaultValue) {
			return config.GetValue<short>(propertyKey, defaultValue);
		}

		[CLSCompliant(false)]
		public static ushort GetUInt16(this IConfiguration config, string propertyKey) {
			return GetUInt16(config, propertyKey, 0);
		}

		[CLSCompliant(false)]
		public static ushort GetUInt16(this IConfiguration config, string propertyKey, ushort defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static int GetInt32(this IConfiguration config, string propertyKey) {
			return GetInt32(config, propertyKey, 0);
		}

		public static int GetInt32(this IConfiguration config, string propertyKey, int defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		[CLSCompliant(false)]
		public static uint GetUInt32(this IConfiguration config, string propertyKey) {
			return GetUInt32(config, propertyKey, 0);
		}

		[CLSCompliant(false)]
		public static uint GetUInt32(this IConfiguration config, string propertyKey, uint defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static long GetInt64(this IConfiguration config, string propertyKey) {
			return GetInt64(config, propertyKey, 0);
		}

		public static long GetInt64(this IConfiguration config, string propertyKey, long defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		[CLSCompliant(false)]
		public static ulong GetUInt64(this IConfiguration config, string propertyKey) {
			return GetUInt64(config, propertyKey, 0);
		}

		[CLSCompliant(false)]
		public static ulong GetUInt64(this IConfiguration config, string propertyKey, ulong defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static bool GetBoolean(this IConfiguration config, string propertyKey) {
			return GetBoolean(config, propertyKey, false);
		}

		public static bool GetBoolean(this IConfiguration config, string propertyKey, bool defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static float GetSingle(this IConfiguration config, string propertyKey) {
			return GetSingle(config, propertyKey, 0);
		}

		public static float GetSingle(this IConfiguration config, string propertyKey, float defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static double GetDouble(this IConfiguration config, string propertyKey) {
			return GetDouble(config, propertyKey, 0);
		}

		public static double GetDouble(this IConfiguration config, string propertyKey, double defaultValue) {
			return config.GetValue<double>(propertyKey, defaultValue);
		}

#endregion

#endregion

		#region Load / Save

		public static void Load(this IConfiguration config, IConfigurationSource source) {
			config.Load(source, new PropertiesConfigurationFormatter());
		}

		public static void Load(this IConfiguration config, IConfigurationFormatter formatter) {
			if (config.Source == null)
				throw new InvalidOperationException("Source was not configured");

			config.Load(config.Source, formatter);
		}

		public static void Load(this IConfiguration config, IConfigurationSource source, IConfigurationFormatter formatter) {
			try {
				if (source != null) {
					using (var sourceStream = source.InputStream) {
						if (!sourceStream.CanRead)
							throw new ArgumentException("The input stream cannot be read.");

						sourceStream.Seek(0, SeekOrigin.Begin);
						formatter.LoadInto(config, sourceStream);
					}
				}
			} catch (Exception ex) {
				throw new DatabaseConfigurationException(String.Format("Cannot load data from source"), ex);
			}
		}

#if !PCL
		public static void Load(this IConfiguration config, string fileName, IConfigurationFormatter formatter) {
			config.Load(new FileConfigurationSource(fileName), formatter);
		}

		public static void Load(this IConfiguration config, string fileName) {
			config.Load(fileName, new PropertiesConfigurationFormatter());
		}
#endif

		public static void Load(this IConfiguration config, Stream inputStream, IConfigurationFormatter formatter) {
			config.Load(new StreamConfigurationSource(inputStream), formatter);
		}

		public static void Load(this IConfiguration config, Stream inputStream) {
			config.Load(inputStream, new PropertiesConfigurationFormatter());
		}

		public static void Save(this IConfiguration config, IConfigurationSource source, IConfigurationFormatter formatter) {
			Save(config, source, ConfigurationLevel.Current, formatter);
		}

		public static void Save(this IConfiguration config, IConfigurationSource source, ConfigurationLevel level,
			IConfigurationFormatter formatter) {
			try {
				var outputStream = source.OutputStream;
				if (!outputStream.CanWrite)
					throw new InvalidOperationException("The destination source cannot be written.");

				outputStream.Seek(0, SeekOrigin.Begin);
				formatter.SaveFrom(config, level, outputStream);
				outputStream.Flush();
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Cannot save the configuration.", ex);
			}
		}

		public static void Save(this IConfiguration config, IConfigurationFormatter formatter) {
			Save(config, ConfigurationLevel.Current, formatter);
		}

		public static void Save(this IConfiguration config, ConfigurationLevel level, IConfigurationFormatter formatter) {
			if (config.Source == null)
				throw new DatabaseConfigurationException("The source was not configured in the configuration.");

			config.Save(config.Source, level, formatter);
		}

		public static void Save(this IConfiguration config) {
			Save(config, ConfigurationLevel.Current);
		}

		public static void Save(this IConfiguration config, ConfigurationLevel level) {
			Save(config, level, new PropertiesConfigurationFormatter());
		}

#if !PCL
		public static void Save(this IConfiguration config, string fileName) {
			Save(config, ConfigurationLevel.Current, fileName);
		}

		public static void Save(this IConfiguration config, ConfigurationLevel level, string fileName) {
			Save(config, level, fileName, new PropertiesConfigurationFormatter());
		}

		public static void Save(this IConfiguration config, string fileName, IConfigurationFormatter formatter) {
			Save(config, ConfigurationLevel.Current, fileName, formatter);
		}

		public static void Save(this IConfiguration config, ConfigurationLevel level, string fileName, IConfigurationFormatter formatter) {
			using (var source = new FileConfigurationSource(fileName)) {
				config.Save(source, level, formatter);
			}
		}
#endif

		public static void Save(this IConfiguration config, Stream outputStream) {
			Save(config, ConfigurationLevel.Current, outputStream);
		}

		public static void Save(this IConfiguration config, ConfigurationLevel level, Stream outputStream) {
			Save(config, level, outputStream, new PropertiesConfigurationFormatter());
		}

		public static void Save(this IConfiguration config, Stream outputStream, IConfigurationFormatter formatter) {
			Save(config, ConfigurationLevel.Current, outputStream, formatter);
		}

		public static void Save(this IConfiguration config, ConfigurationLevel level, Stream outputStream, IConfigurationFormatter formatter) {
			config.Save(new StreamConfigurationSource(outputStream), level, formatter);
		}

		#endregion

		public static IConfiguration MergeWith(this IConfiguration configuration, IConfiguration other) {
			var newConfig = new Configuration(configuration);
			foreach (var pair in other) {
				newConfig.SetValue(pair.Key, pair.Value);
			}

			return newConfig;
		}
	}
}