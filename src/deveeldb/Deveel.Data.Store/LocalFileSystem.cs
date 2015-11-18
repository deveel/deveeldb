using System;
using System.IO;

using Deveel.Data.Sql.Fluid;

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
			return CreateFile(path);
		}

		public LocalFile CreateFile(string fileName) {
			if (String.IsNullOrEmpty(fileName))
				throw new ArgumentNullException("fileName");

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

		public bool RenameFile(string sourcePath, string destPath) {
			File.Move(sourcePath, destPath);
			return File.Exists(destPath);
		}
	}
}
