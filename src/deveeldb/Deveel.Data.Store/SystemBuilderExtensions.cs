using System;

using Deveel.Data.Build;
using Deveel.Data.Services;
using Deveel.Data.Store.Journaled;

namespace Deveel.Data.Store {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseStoreSystem<T>(this ISystemBuilder builder, string key) where T : class, IStoreSystem {
			return builder.Use<IStoreSystem>(options => options
				.With<T>()
				.HavingKey(key)
				.InDatabaseScope());

			return builder;
		}

		public static ISystemBuilder UseInMemoryStoreSystem(this ISystemBuilder builder) {
			return builder.UseStoreSystem<InMemoryStorageSystem>(DefaultStorageSystemNames.Heap);
		}

		public static ISystemBuilder UseSingleFileStoreSystem(this ISystemBuilder builder) {
			return builder.UseStoreSystem<SingleFileStoreSystem>(DefaultStorageSystemNames.SingleFile);
		}

		public static ISystemBuilder UseJournaledStoreSystem(this ISystemBuilder builder) {
			return builder.UseStoreSystem<JournaledStoreSystem>(DefaultStorageSystemNames.Journaled);
		}

		public static ISystemBuilder UseFileSystem<T>(this ISystemBuilder builder, string key) where T : class, IFileSystem {
			return builder.Use<IFileSystem>(options => options
				.With<T>()
				.HavingKey(key)
				.InSystemScope());
		}

		public static ISystemBuilder UseLocalFileSystem(this ISystemBuilder builder) {
#if PCL
			return builder.UseFileSystem<PortableFileSystem>("local");
#else
			return builder.UseFileSystem<LocalFileSystem>("local");
#endif
		}

		public static ISystemBuilder UseStoreDataFactory<T>(this ISystemBuilder builder, string key)
			where T : class, IStoreDataFactory {
			return builder.Use<IStoreDataFactory>(options => options
				.With<T>()
				.HavingKey(key)
				.InDatabaseScope());
		}

		public static ISystemBuilder UseScatteringFileDataFactory(this ISystemBuilder builder) {
			return builder.UseStoreDataFactory<ScatteringFileStoreDataFactory>("scattering");
		}
	}
}
