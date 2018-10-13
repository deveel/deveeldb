// 
//  Copyright 2010-2018 Deveel
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
using System.Reflection;
using System.Text;

namespace Deveel.Data.Configurations {
	public static class ConfigurationExtensions {
		public static IConfiguration GetChild(this IConfiguration configuration, string key) {
			var dict = configuration.Sections.ToDictionary(x => x.Key, y => y.Value);

			IConfiguration child;
			if (!dict.TryGetValue(key, out child))
				return null;

			return child;
		}

		public static IEnumerable<string> GetAllKeys(this IConfiguration configuration) {
			var result = new List<string>();
			GetKeys(configuration, null, result);
			return result;
		}

		private static void GetKeys(IConfiguration configuration, string prefix, List<string> result) {
			var keys = configuration.Keys.Select(x => {
				var sb = new StringBuilder();
				if (!String.IsNullOrEmpty(prefix)) {
					sb.Append(prefix);
					sb.Append(Configuration.SectionSeparator);
				}

				sb.Append(x);
				return sb.ToString();
			});

			result.AddRange(keys);

			foreach (var child in configuration.Sections) {
				var sectionName = new StringBuilder();
				if (!String.IsNullOrEmpty(prefix)) {
					sectionName.Append(prefix);
					sectionName.Append(Configuration.SectionSeparator);
				}
				sectionName.Append(child.Key);

				GetKeys(child.Value, sectionName.ToString(), result);
			}
		}

		#region Get Values

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

			if (typeof(T).GetTypeInfo().IsEnum)
				return ConvertToEnum<T>(value);

			var nullableType = Nullable.GetUnderlyingType(typeof(T));
			if (nullableType == null) {
				value = (T) Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
			} else {
				value = (T) Convert.ChangeType(value, nullableType, CultureInfo.InvariantCulture);
			}

			return (T)value;
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
			return config.GetValue(propertyKey, defaultValue);
		}

		#endregion

		#endregion

		public static IConfiguration MergeWith(this IConfiguration configuration, IConfiguration other) {
			var newConfig = new Configuration();
			foreach (var pair in configuration) {
				newConfig.SetValue(pair.Key, pair.Value);
			}
			foreach (var pair in other) {
				newConfig.SetValue(pair.Key, pair.Value);
			}

			return newConfig;
		}
	}
}