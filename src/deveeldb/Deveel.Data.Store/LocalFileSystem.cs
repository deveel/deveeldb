using System;

namespace Deveel.Data.Store {
	public sealed class LocalFileSystem : IFileSystem {
		public bool FileExists(string path) {
			throw new NotImplementedException();
		}

		IFile IFileSystem.OpenFile(string path, bool readOnly) {
			throw new NotImplementedException();
		}

		IFile IFileSystem.CreateFile(string path) {
			throw new NotImplementedException();
		}

		public bool DeleteFile(string path) {
			throw new NotImplementedException();
		}
	}
}
