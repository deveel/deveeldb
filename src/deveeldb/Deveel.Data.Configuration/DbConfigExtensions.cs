using System;
using System.Globalization;
using System.IO;

namespace Deveel.Data.Configuration {
	public static class DbConfigExtensions {
		public static object GetValue(this IDbConfig config, string propertyKey) {
			return config.GetValue(propertyKey, null);
		}

		public static T GetValue<T>(this IDbConfig config, string propertyKey) {
			return GetValue<T>(config, propertyKey, default(T));
		}

		public static T GetValue<T>(this IDbConfig config, string propertyKey, T defaultValue) {
			object value = config.GetValue(propertyKey, null);
			if (value == null)
				return defaultValue;

			if (!typeof (T).IsInstanceOfType(value) &&
			    value is IConvertible)
				value = Convert.ChangeType(value, typeof (T), CultureInfo.InvariantCulture);

			return (T) value;
		}

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
	}
}