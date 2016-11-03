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
