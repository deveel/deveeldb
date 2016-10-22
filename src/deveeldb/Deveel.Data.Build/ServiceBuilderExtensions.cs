using System;

namespace Deveel.Data.Build {
	public static class ServiceBuilderExtensions {
		public static ISystemBuilder Use<TService>(this ISystemBuilder builder, object key) where TService : class {
			return builder.Use<TService>(config => config.ToSelf().HavingKey(key));
		}

		public static ISystemBuilder Use<TService>(this ISystemBuilder builder) where TService : class {
			return builder.Use<TService>((object)null);
		}

		public static ISystemBuilder Use<TService, TImplementation>(this ISystemBuilder builder, object key)
			where TImplementation : class, TService {
			return builder.Use<TService>(config => config.With<TImplementation>().HavingKey(key));
		}

		public static ISystemBuilder Use<TService, TImplementation>(this ISystemBuilder builder)
			where TImplementation : class, TService {
			return builder.Use<TService, TImplementation>(null);
		}


		public static ISystemBuilder Use<TService>(this ISystemBuilder builder, TService service) where TService : class {
			return Use<TService>(builder, null, service);
		}

		public static ISystemBuilder Use<TService>(this ISystemBuilder builder, object key, TService service) where TService : class {
			return builder.Use<TService>(options => options.With(service).HavingKey(key));
		}

		public static ISystemBuilder Use<TService>(this ISystemBuilder builder, Action<IServiceUseConfiguration<TService>> options) {
			var provider = new ServiceUseConfigurationProvider<TService>();
			options(provider);

			return builder.Use(provider.Options);
		}

		public static ISystemBuilder UseFeature<TFeature>(this ISystemBuilder builder, TFeature feature) where TFeature : class, ISystemFeature {
			return builder.Use<ISystemFeature>(options => options.With(feature));
		}

		public static ISystemBuilder UseFeature(this ISystemBuilder builder, Action<FeatureBuilder> feature) {
			var featureBuilder = new FeatureBuilder();
			feature(featureBuilder);

			return builder.UseFeature(featureBuilder.Build());
		}

		#region ServiceUseConfigurationProvider

		class ServiceUseConfigurationProvider<TService> : IServiceUseConfiguration<TService> {
			public ServiceUseOptions Options { get; private set; }

			public ServiceUseConfigurationProvider() {
				Options = new ServiceUseOptions(typeof(TService));
			}

			public IServiceUseWithBindingConfiguration<TService, TImplementation> With<TImplementation>() 
				where TImplementation : class, TService {
				Options.ImplementationType = typeof(TImplementation);
				return new ServiceUseWithBindingConfigurationProvider<TService, TImplementation>(Options);
			}

			public IServiceUseWithBindingConfiguration<TService, TImplementation> With<TImplementation>(TImplementation instance) 
				where TImplementation : class, TService {
				Options.Instance = instance;
				return new ServiceUseWithBindingConfigurationProvider<TService, TImplementation>(Options);
			}
		}

		#endregion

		#region ServiceUseWithBindingConfigurationProvider

		class ServiceUseWithBindingConfigurationProvider<TService, TImplementation> : IServiceUseWithBindingConfiguration<TService, TImplementation> 
			where TImplementation : class, TService {
			private readonly ServiceUseOptions options;

			public ServiceUseWithBindingConfigurationProvider(ServiceUseOptions options) {
				this.options = options;
			}

			public IServiceUseWithBindingConfiguration<TService, TImplementation> HavingKey(object key) {
				options.Key = key;
				return this;
			}

			public IServiceUseWithBindingConfiguration<TService, TImplementation> InScope(string scope) {
				if (!String.IsNullOrEmpty(scope))
					options.Scope = scope;

				return this;
			}

			public IServiceUseWithBindingConfiguration<TService, TImplementation> Replace(bool replace = true) {
				options.Policy = replace ? ServiceUsePolicy.Replace : ServiceUsePolicy.Bind;
				return this;
			}
		}

		#endregion
	}
}
