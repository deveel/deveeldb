using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

using Deveel.Data.Caching;
using Deveel.Data.Security;

namespace Deveel.Data.Configuration {
	public static class DbConfigExtensions {
		#region GetValue Extensions

		public static object GetValue(this IDbConfig config, string propertyKey) {
			return config.GetValue(propertyKey, null);
		}

		public static T GetValue<T>(this IDbConfig config, string propertyKey) {
			return GetValue(config, propertyKey, default(T));
		}

		public static T GetValue<T>(this IDbConfig config, string propertyKey, T defaultValue) {
			object value = config.GetValue(propertyKey, null);
			if (value == null)
				return defaultValue;

			if (!(value is T) && value is IConvertible)
				value = Convert.ChangeType(value, typeof (T), CultureInfo.InvariantCulture);

			return (T) value;
		}

		public static string GetString(this IDbConfig config, string propertyKey) {
			return GetString(config, propertyKey, null);
		}

		public static string GetString(this IDbConfig config, string propertyKey, string defaultValue) {
			return config.GetValue<string>(propertyKey, defaultValue);
		}

		public static byte GetByte(this IDbConfig config, string propertyKey) {
			return GetByte(config, propertyKey, 0);
		}

		public static byte GetByte(this IDbConfig config, string propertyKey, byte defaultValue) {
			return config.GetValue<byte>(propertyKey, defaultValue);
		}

		[CLSCompliant(false)]
		public static sbyte GetSByte(this IDbConfig config, string propertyKey, sbyte defaultValue) {
			return config.GetValue<sbyte>(propertyKey, defaultValue);
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
			return config.GetValue<ushort>(propertyKey, defaultValue);
		}

		public static int GetInt32(this IDbConfig config, string propertyKey) {
			return GetInt32(config, propertyKey, 0);
		}

		public static int GetInt32(this IDbConfig config, string propertyKey, int defaultValue) {
			return config.GetValue<int>(propertyKey, defaultValue);
		}

		[CLSCompliant(false)]
		public static uint GetUInt32(this IDbConfig config, string propertyKey) {
			return GetUInt32(config, propertyKey, 0);
		}

		[CLSCompliant(false)]
		public static uint GetUInt32(this IDbConfig config, string propertyKey, uint defaultValue) {
			return config.GetValue<uint>(propertyKey, defaultValue);
		}

		public static long GetInt64(this IDbConfig config, string propertyKey) {
			return GetInt64(config, propertyKey, 0);
		}

		public static long GetInt64(this IDbConfig config, string propertyKey, long defaultValue) {
			return config.GetValue<long>(propertyKey, defaultValue);
		}

		[CLSCompliant(false)]
		public static ulong GetUInt64(this IDbConfig config, string propertyKey) {
			return GetUInt64(config, propertyKey, 0);
		}

		[CLSCompliant(false)]
		public static ulong GetUInt64(this IDbConfig config, string propertyKey, ulong defaultValue) {
			return config.GetValue<ulong>(propertyKey, defaultValue);
		}

		public static bool GetBoolean(this IDbConfig config, string propertyKey) {
			return GetBoolean(config, propertyKey, false);
		}

		public static bool GetBoolean(this IDbConfig config, string propertyKey, bool defaultValue) {
			var value = config.GetValue(propertyKey);
			if (value == null)
				return defaultValue;

			if (value is bool)
				return (bool) value;
			if (value is string) {
				if (String.Equals((string) value, "true", StringComparison.OrdinalIgnoreCase) ||
				    String.Equals((string) value, "enabled", StringComparison.OrdinalIgnoreCase) ||
				    String.Equals((string) value, "1"))
					return true;
				if (String.Equals((string) value, "false", StringComparison.OrdinalIgnoreCase) ||
				    String.Equals((string) value, "disabled", StringComparison.OrdinalIgnoreCase) ||
				    String.Equals((string) value, "0"))
					return false;
			}
			if (value is IConvertible)
				value = Convert.ChangeType(value, typeof (bool), CultureInfo.InvariantCulture);

			return (value == null ? defaultValue : (bool) value);
		}

		public static float GetSingle(this IDbConfig config, string propertyKey) {
			return GetSingle(config, propertyKey, 0);
		}

		public static float GetSingle(this IDbConfig config, string propertyKey, float defaultValue) {
			return config.GetValue<float>(propertyKey, defaultValue);
		}

		public static double GetDouble(this IDbConfig config, string propertyKey) {
			return GetDouble(config, propertyKey, 0);
		}

		public static double GetDouble(this IDbConfig config, string propertyKey, double defaultValue) {
			return config.GetValue<double>(propertyKey, defaultValue);
		}

		#endregion

		#region Default System Configurations

		public static string DatabasePath(this IDbConfig config) {
			return config.GetString(ConfigKeys.DatabasePath);
		}

		public static void DatabasePath(this IDbConfig config, string value) {
			config.SetValue(ConfigKeys.DatabasePath, value);
		}

		public static string StorageSystem(this IDbConfig config) {
			return config.GetString(ConfigKeys.StorageSystem, ConfigDefaultValues.HeapStorageSystem);
		}

		public static void StorageSystem(this IDbConfig config, string value) {
			config.SetValue(ConfigKeys.StorageSystem, value);
		}

		public static string BasePath(this IDbConfig config) {
			return config.GetString(ConfigKeys.BasePath);
		}

		public static void BasePath(this IDbConfig config, string value) {
			config.SetValue(ConfigKeys.BasePath, value);
		}

		public static bool ReadOnly(this IDbConfig config) {
			return config.GetBoolean(ConfigKeys.ReadOnly);
		}

		public static void ReadOnly(this IDbConfig config, bool value) {
			config.SetValue(ConfigKeys.ReadOnly, value);
		}

		public static bool IgnoreIdentifierCase(this IDbConfig config) {
			return config.GetBoolean(ConfigKeys.IgnoreIdentifiersCase);
		}

		public static void IgnoreIdentifierCase(this IDbConfig config, bool value) {
			config.SetValue(ConfigKeys.IgnoreIdentifiersCase, value);
		}

		public static string LogPath(this IDbConfig config) {
			return config.GetString(ConfigKeys.LogPath);
		}

		public static void LogPath(this IDbConfig config, string value) {
			config.SetValue(ConfigKeys.LogPath, value);
		}

		public static Type CacheType(this IDbConfig config) {
			var typeString = config.GetString(ConfigKeys.CacheType, ConfigDefaultValues.HeapCache);
			if (String.IsNullOrEmpty(typeString))
				return null;

			if (String.Equals(typeString, ConfigDefaultValues.HeapCache, StringComparison.OrdinalIgnoreCase))
				return typeof(MemoryCache);

			return Type.GetType(typeString, false, true);
		}

		public static void CacheType(this IDbConfig config, Type type) {
			string typeString = null;
			if (type != null)
				typeString = type.AssemblyQualifiedName;
			config.SetValue(ConfigKeys.CacheType, typeString);
		}

		/// <summary>
		/// Parses a file string to an absolute position in the file system.
		/// </summary>
		/// <remarks>
		/// We must provide the path to the root directory (eg. the directory 
		/// where the config bundle is located).
		/// </remarks>
		public static string ParseFileString(this IDbConfig config, string rootInfo, string pathString) {
			string path = Path.GetFullPath(pathString);
			string res;
			// If the path is absolute then return the absoluate reference
			if (Path.IsPathRooted(pathString)) {
				res = path;
			} else {
				// If the root path source is the environment then just return the path.
				if (rootInfo != null && rootInfo.Equals("env"))
					return path;

				// If the root path source is the configuration file then
				// concat the configuration path with the path string and return it.

				res = Path.Combine(config.BasePath(), pathString);
			}

			return res;
		}

		public static int DataCacheSize(this IDbConfig config) {
			return config.GetInt32(ConfigKeys.DataCacheSize);
		}

		public static int MaxCacheEntrySize(this IDbConfig config) {
			return config.GetInt32(ConfigKeys.MaxCacheEntrySize);
		}

		public static string PasswordHashFunction(this IDbConfig config) {
			return config.GetString(ConfigKeys.PasswordHashFunction, ConfigDefaultValues.PasswordHashFunction);
		}

		public static void PasswordHashFunction(this IDbConfig config, string value) {
			config.SetValue(ConfigKeys.PasswordHashFunction, value);
		}

		public static bool TransactionErrorOnDirtySelect(this IDbConfig config) {
			return config.GetBoolean(ConfigKeys.TransactionErrorOnDirtySelect, ConfigDefaultValues.TransactionErrorOnDirtySelect);
		}

		public static void TransactionErrorOnDirtySelect(this IDbConfig config, bool value) {
			config.SetValue(ConfigKeys.TransactionErrorOnDirtySelect, value);
		}

		public static string DefaultSchema(this IDbConfig config) {
			return config.GetString(ConfigKeys.DefaultSchema, ConfigDefaultValues.DefaultSchema);
		}

		public static void DefaultSchema(this IDbConfig config, string value) {
			config.SetValue(ConfigKeys.DefaultSchema, value);
		}

		public static bool LogQueries(this IDbConfig config) {
			return config.GetBoolean(ConfigKeys.LogQueries, ConfigDefaultValues.LogQueries);
		}

		public static void LogQueries(this IDbConfig config, bool value) {
			config.SetValue(ConfigKeys.LogQueries, value);
		}

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
			if (source != null) {
				using (var sourceStream = source.InputStream) {
					if (!sourceStream.CanRead)
						throw new ArgumentException("The input stream cannot be read.");

					sourceStream.Seek(0, SeekOrigin.Begin);
					formatter.LoadInto(config, sourceStream);
				}
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
			using (var outputStream = source.OutputStream) {
				if (!outputStream.CanWrite)
					throw new InvalidOperationException("The destination source cannot be written.");

				outputStream.Seek(0, SeekOrigin.Begin);
				formatter.SaveFrom(config, outputStream);
				outputStream.Flush();
			}
		}

		public static void Save(this IDbConfig config, IConfigFormatter formatter) {
			if (config.Source == null)
				throw new InvalidOperationException("Source was not configured.");

			config.Save(config.Source, formatter);
		}

		public static void Save(this IDbConfig config) {
			Save(config, new PropertiesConfigFormatter());
		}

		public static void Save(this IDbConfig config, string fileName) {
			Save(config, fileName, new PropertiesConfigFormatter());
		}

		public static void Save(this IDbConfig config, string fileName, IConfigFormatter formatter) {
			config.Save(new FileConfigSource(fileName), formatter);
		}

		public static void Save(this IDbConfig config, Stream outputStream) {
			Save(config, outputStream, new PropertiesConfigFormatter());
		}

		public static void Save(this IDbConfig config, Stream outputStream, IConfigFormatter formatter) {
			config.Save(new StreamConfigSource(outputStream), formatter);
		}

		#endregion

		#region Subset

		public static DbConfig Subset(this IDbConfig config, string prefix) {
			var subsetConfig = new DbConfig(config);

			foreach (KeyValuePair<string, object> pair in config) {
				var key = pair.Key;
				var sepIndex = key.IndexOf('.');
				if (sepIndex == -1)
					continue;

				var subPrefix = key.Substring(0, sepIndex);
				if (!String.Equals(prefix, subPrefix))
					continue;

				var subKey = key.Substring(sepIndex + 1);
				config.SetValue(subKey, pair.Value);
			}

			return subsetConfig;
		}

		#endregion
	}
}