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

namespace Deveel.Data.Store {
	public sealed class ScatteringFileStoreDataFactory : IStoreDataFactory {
		public ScatteringFileStoreDataFactory(IFileSystem fileSystem, string basePath, int maxSliceSize, string fileExt) {
			if (String.IsNullOrEmpty(basePath))
				throw new ArgumentNullException("basePath");
			if (fileSystem == null)
				throw new ArgumentNullException("fileSystem");

			FileSystem = fileSystem;
			BasePath = basePath;
			MaxSliceSize = maxSliceSize;
			FileExtension = fileExt;
		}

		public string BasePath { get; private set; }

		public string FileExtension { get; private set; }

		public int MaxSliceSize { get; private set; }

		public IFileSystem FileSystem { get; private set; }

		public IStoreData CreateData(string name) {
			return new ScatteringFileStoreData(FileSystem, BasePath, name, FileExtension, MaxSliceSize);
		}
	}
}
