using System;
using System.IO;

namespace Deveel.Data.Store {
	public sealed class LocalFileSystem : IFileSystem {
		public bool FileExists(string path) {
			return File.Exists(path);
		}

		IFile IFileSystem.OpenFile(string path, bool readOnly) {
			return OpenFile(path, readOnly);
		}

		public LocalFile OpenFile(string fileName, bool readOnly) {
			if (!FileExists(fileName))
				throw new IOException(string.Format("The file '{0}' does not exist: cannot be opened", fileName));

			return new LocalFile(fileName, readOnly);
		}

		IFile IFileSystem.CreateFile(string path) {
			throw new NotImplementedException();
		}

		public LocalFile CreateFile(string fileName) {
			if (FileExists(fileName))
				throw new IOException(string.Format("The file '{0}' already exists: cannot create.", fileName));

			return new LocalFile(fileName, false);
		}

		public bool DeleteFile(string path) {
			File.Delete(path);
			return FileExists(path);
		}

		public string CombinePath(string path1, string path2) {
			return Path.Combine(path1, path2);
		}
	}
}
