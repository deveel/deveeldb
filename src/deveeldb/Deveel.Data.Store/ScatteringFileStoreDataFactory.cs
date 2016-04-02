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

using Deveel.Data.Configuration;

namespace Deveel.Data.Store {
	public sealed class ScatteringFileStoreDataFactory : IStoreDataFactory {
		public ScatteringFileStoreDataFactory(IDatabaseContext context) {
			Configure(context);
		}

		public string BasePath { get; private set; }

		public string FileExtension { get; private set; }

		public int MaxSliceSize { get; private set; }

		public IFileSystem FileSystem { get; private set; }

		public IStoreData CreateData(string name) {
			return new ScatteringFileStoreData(FileSystem, BasePath, name, FileExtension, MaxSliceSize);
		}

		private void Configure(IContext context) {
			var configuration = context.ResolveService<IConfiguration>();
			if (configuration == null)
				throw new DatabaseConfigurationException("No configuration found in context.");

			BasePath = configuration.GetString("store.dataFactory.scattering.basePath");
			if (String.IsNullOrEmpty(BasePath))
				BasePath = configuration.GetString("database.path");
			if (String.IsNullOrEmpty(BasePath))
				throw new DatabaseConfigurationException("No base path was set for the data factory.");

			FileExtension = configuration.GetString("store.dataFactory.scattering.fileExtension", ".db");

			// 1Gb by default
			const int defaultMaxSliceSize = 16384*65536;
			MaxSliceSize = configuration.GetInt32("store.dataFactory.scattering.maxSliceSize", defaultMaxSliceSize);

			var fileSystemName = configuration.GetString("store.dataFactory.scattering.fileSystem");
			if (String.IsNullOrEmpty(fileSystemName)) {
				fileSystemName = configuration.GetString("store.fileSystem", "local");
			}

			FileSystem = context.ResolveService<IFileSystem>(fileSystemName);
			if (FileSystem == null)
				throw new DatabaseConfigurationException(String.Format("File system '{0}' was not found in context.", fileSystemName));

		}
	}
}
