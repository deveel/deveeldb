using System;

namespace Deveel.Data.Configuration {
	public static class ConfigurationBuilderExtensions {
		public static IConfigurationBuilder Load(this IConfigurationBuilder builder, IConfigurationSource source,
			IConfigurationFormatter formatter) {
			var config = new Configuration();
			formatter.LoadInto(config, source.InputStream);

			foreach (var pair in config) {
				builder = builder.WithSetting(pair.Key, pair.Value);
			}

			return builder;
		}

		public static IConfigurationBuilder Load(this IConfigurationBuilder builder, IConfigurationSource source) {
			return builder.Load(source, new PropertiesConfigurationFormatter());
		}

		public static IConfigurationBuilder LoadFile(this IConfigurationBuilder builder, string fileName,
			IConfigurationFormatter formatter) {
			using (var source = new FileConfigurationSource(fileName)) {
				return builder.Load(source, formatter);
			}
		}

		public static IConfigurationBuilder LoadFile(this IConfigurationBuilder builder, string fileName) {
			return builder.LoadFile(fileName, new PropertiesConfigurationFormatter());
		}
	}
}
