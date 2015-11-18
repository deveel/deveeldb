using System;

namespace Deveel.Data.Store {
	public interface IFileSystem {
		bool FileExists(string path);

		IFile OpenFile(string path, bool readOnly);

		IFile CreateFile(string path);

		bool DeleteFile(string path);

		string CombinePath(string path1, string path2);

		bool RenameFile(string sourcePath, string destPath);
	}
}
