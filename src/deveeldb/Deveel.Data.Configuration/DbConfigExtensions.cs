// 
//  Copyright 2010-2015 Deveel
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

namespace Deveel.Data.Configuration {
	public static class DbConfigExtensions {
		#region Get Values

		public static IEnumerable<ConfigKey> GetKeys(this IDbConfig config) {
			return config.GetKeys(ConfigurationLevel.Current);
		}

		public static IEnumerable<ConfigValue> GetValues(this IDbConfig config, ConfigurationLevel level) {
			var keys = config.GetKeys(level);
			var values = keys.Select(config.GetValue)
				.Where(value => value != null)
				.ToList();

			return values.AsReadOnly();
		}

		public static ConfigValue GetConfigValue(this IDbConfig config, string keyName) {
			var key = config.GetKey(keyName);
			if (key == null)
				return null;

			return config.GetValue(key);
		}

		#region GetValue(ConfigKey)

		public static T GetValue<T>(this IDbConfig config, ConfigKey key) {
			var value = config.GetValue(key);
			if (value == null)
				return default(T);

			return value.ToType<T>();
		}

		public static string GetString(this IDbConfig config, ConfigKey key) {
			return config.GetValue<string>(key);
		}

		public static byte GetByte(this IDbConfig config, ConfigKey key) {
			return config.GetValue<byte>(key);
		}

		[CLSCompliant(false)]
		public static sbyte GetSByte(this IDbConfig config, ConfigKey key) {
			return config.GetValue<sbyte>(key);
		}

		public static short GetInt16(this IDbConfig config, ConfigKey key) {
			return config.GetValue<short>(key);
		}

		[CLSCompliant(false)]
		public static ushort GetUInt16(this IDbConfig config, ConfigKey key) {
			return config.GetValue<ushort>(key);
		}

		public static int GetInt32(this IDbConfig config, ConfigKey key) {
			return config.GetValue<int>(key);
		}

		[CLSCompliant(false)]
		public static uint GetUInt32(this IDbConfig config, ConfigKey key) {
			return config.GetValue<uint>(key);
		}

		public static long GetInt64(this IDbConfig config, ConfigKey key) {
			return config.GetValue<long>(key);
		}

		[CLSCompliant(false)]
		public static ulong GetUInt64(this IDbConfig config, ConfigKey key) {
			return config.GetValue<ulong>(key);
		}

		public static bool GetBoolean(this IDbConfig config, ConfigKey key) {
			var value = config.GetValue(key);
			if (value == null)
				return false;

			if (value.Value is bool)
				return (bool)value.Value;

			try {
				if (value.Value is string) {
					if (String.Equals((string)value.Value, "true", StringComparison.OrdinalIgnoreCase) ||
						String.Equals((string)value.Value, "enabled", StringComparison.OrdinalIgnoreCase) ||
						String.Equals((string)value.Value, "1"))
						return true;
					if (String.Equals((string)value.Value, "false", StringComparison.OrdinalIgnoreCase) ||
						String.Equals((string)value.Value, "disabled", StringComparison.OrdinalIgnoreCase) ||
						String.Equals((string)value.Value, "0"))
						return false;
				}

				return value.ToType<bool>();
			} catch (Exception e) {
				throw new DatabaseConfigurationException(String.Format("Cannot convert {0} to a valid boolean", value), e);
			}
		}

		public static float GetSingle(this IDbConfig config, ConfigKey key) {
			return config.GetValue<float>(key);
		}

		public static double GetDouble(this IDbConfig config, ConfigKey key) {
			return config.GetValue<double>(key);
		}

		#endregion

		#region GetValue(string)

		public static object GetValue(this IDbConfig config, string keyName) {
			return GetValue(config, keyName, null);
		}

		public static object GetValue(this IDbConfig config, string keyName, object defaultValue) {
			var value = config.GetConfigValue(keyName);
			if (value == null)
				return defaultValue;

			return value.Value;
		}

		public static T GetValue<T>(this IDbConfig config, string keyName) {
			return GetValue<T>(config, keyName, default(T));
		}

		public static T GetValue<T>(this IDbConfig config, string keyName, T defaultValue) {
			var value = config.GetConfigValue(keyName);
			if (value == null)
				return defaultValue;

			return value.ToType<T>();
		}

		public static string GetString(this IDbConfig config, string propertyKey) {
			return GetString(config, propertyKey, null);
		}

		public static string GetString(this IDbConfig config, string propertyKey, string defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static byte GetByte(this IDbConfig config, string propertyKey) {
			return GetByte(config, propertyKey, 0);
		}

		public static byte GetByte(this IDbConfig config, string propertyKey, byte defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		[CLSCompliant(false)]
		public static sbyte GetSByte(this IDbConfig config, string propertyKey, sbyte defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static short GetInt16(this IDbConfig config, string propertyKey) {
			return GetInt16(config, propertyKey, 0);
		}

		public static short GetInt16(this IDbConfig config, string propertyKey, short defaultValue) {
			return config.GetValue<short>(propertyKey, defaultValue);
		}

		[CLSCompliant(false)]
		public static ushort GetUInt16(this IDbConfig config, string propertyKey) {
			return GetUInt16(config, propertyKey, 0);
		}

		[CLSCompliant(false)]
		public static ushort GetUInt16(this IDbConfig config, string propertyKey, ushort defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static int GetInt32(this IDbConfig config, string propertyKey) {
			return GetInt32(config, propertyKey, 0);
		}

		public static int GetInt32(this IDbConfig config, string propertyKey, int defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		[CLSCompliant(false)]
		public static uint GetUInt32(this IDbConfig config, string propertyKey) {
			return GetUInt32(config, propertyKey, 0);
		}

		[CLSCompliant(false)]
		public static uint GetUInt32(this IDbConfig config, string propertyKey, uint defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static long GetInt64(this IDbConfig config, string propertyKey) {
			return GetInt64(config, propertyKey, 0);
		}

		public static long GetInt64(this IDbConfig config, string propertyKey, long defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		[CLSCompliant(false)]
		public static ulong GetUInt64(this IDbConfig config, string propertyKey) {
			return GetUInt64(config, propertyKey, 0);
		}

		[CLSCompliant(false)]
		public static ulong GetUInt64(this IDbConfig config, string propertyKey, ulong defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static bool GetBoolean(this IDbConfig config, string propertyKey) {
			return GetBoolean(config, propertyKey, false);
		}

		public static bool GetBoolean(this IDbConfig config, string propertyKey, bool defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static float GetSingle(this IDbConfig config, string propertyKey) {
			return GetSingle(config, propertyKey, 0);
		}

		public static float GetSingle(this IDbConfig config, string propertyKey, float defaultValue) {
			return config.GetValue(propertyKey, defaultValue);
		}

		public static double GetDouble(this IDbConfig config, string propertyKey) {
			return GetDouble(config, propertyKey, 0);
		}

		public static double GetDouble(this IDbConfig config, string propertyKey, double defaultValue) {
			return config.GetValue<double>(propertyKey, defaultValue);
		}

		#endregion

		#endregion

		#region Load / Save

		public static void Load(this IDbConfig config, IConfigSource source) {
			config.Load(source, new PropertiesConfigFormatter());
		}

		public static void Load(this IDbConfig config, IConfigFormatter formatter) {
			if (config.Source == null)
				throw new InvalidOperationException("Source was not configured");

			config.Load(config.Source, formatter);
		}

		public static void Load(this IDbConfig config, IConfigSource source, IConfigFormatter formatter) {
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

		public static void Load(this IDbConfig config, string fileName, IConfigFormatter formatter) {
			config.Load(new FileConfigSource(fileName), formatter);
		}

		public static void Load(this IDbConfig config, string fileName) {
			config.Load(fileName, new PropertiesConfigFormatter());
		}

		public static void Load(this IDbConfig config, Stream inputStream, IConfigFormatter formatter) {
			config.Load(new StreamConfigSource(inputStream), formatter);
		}

		public static void Load(this IDbConfig config, Stream inputStream) {
			config.Load(inputStream, new PropertiesConfigFormatter());
		}

		public static void Save(this IDbConfig config, IConfigSource source, IConfigFormatter formatter) {
			Save(config, source, ConfigurationLevel.Current, formatter);
		}

		public static void Save(this IDbConfig config, IConfigSource source, ConfigurationLevel level, IConfigFormatter formatter) {
			try {
				using (var outputStream = source.OutputStream) {
					if (!outputStream.CanWrite)
						throw new InvalidOperationException("The destination source cannot be written.");

					outputStream.Seek(0, SeekOrigin.Begin);
					formatter.SaveFrom(config, level, outputStream);
					outputStream.Flush();
				}
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Cannot save the configuration.", ex);
			}
		}

		public static void Save(this IDbConfig config, IConfigFormatter formatter) {
			Save(config, ConfigurationLevel.Current, formatter);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level, IConfigFormatter formatter) {
			if (config.Source == null)
				throw new DatabaseConfigurationException("The source was not configured in the configuration.");

			config.Save(config.Source, level, formatter);
		}

		public static void Save(this IDbConfig config) {
			Save(config, ConfigurationLevel.Current);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level) {
			Save(config, level, new PropertiesConfigFormatter());
		}

		public static void Save(this IDbConfig config, string fileName) {
			Save(config, ConfigurationLevel.Current, fileName);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level, string fileName) {
			Save(config, level, fileName, new PropertiesConfigFormatter());
		}

		public static void Save(this IDbConfig config, string fileName, IConfigFormatter formatter) {
			Save(config, ConfigurationLevel.Current, fileName, formatter);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level, string fileName, IConfigFormatter formatter) {
			config.Save(new FileConfigSource(fileName), level, formatter);
		}

		public static void Save(this IDbConfig config, Stream outputStream) {
			Save(config, ConfigurationLevel.Current, outputStream);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level, Stream outputStream) {
			Save(config, level, outputStream, new PropertiesConfigFormatter());
		}

		public static void Save(this IDbConfig config, Stream outputStream, IConfigFormatter formatter) {
			Save(config, ConfigurationLevel.Current, outputStream, formatter);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level, Stream outputStream, IConfigFormatter formatter) {
			config.Save(new StreamConfigSource(outputStream), level, formatter);
		}

		#endregion

		public static void CopyTo(this IDbConfig source, IDbConfig dest) {
			var sourceKeys = source.GetKeys(ConfigurationLevel.Current);
			foreach (var key in sourceKeys) {
				dest.SetKey(key);

				var value = source.GetValue(key);
				if (value != null)
					dest.SetValue(key, value.Value);
			}
		}
	}
}