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
