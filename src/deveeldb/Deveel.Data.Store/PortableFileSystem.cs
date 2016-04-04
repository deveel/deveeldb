using System;
using System.IO;

using Irony.Parsing;

using PCLStorage;

namespace Deveel.Data.Store {
	public sealed class PortableFileSystem : IFileSystem {
		public PortableFileSystem() {
			CurrentFileSystem = PCLStorage.FileSystem.Current;
		}

		private PCLStorage.IFileSystem CurrentFileSystem { get; set; }

		public bool FileExists(string path) {
			var task = CurrentFileSystem.GetFileFromPathAsync(path);
			task.RunSynchronously();
			return task.Result != null;
		}

		public IFile OpenFile(string path, bool readOnly) {
			try {
				var task = CurrentFileSystem.GetFileFromPathAsync(path);
				task.RunSynchronously();

				var file = task.Result;
				if (file == null)
					throw new ArgumentException(String.Format("The file '{0}' does not exist in the current file-system.", path));

				var access = readOnly ? FileAccess.Read : FileAccess.ReadAndWrite;
				var openTask = file.OpenAsync(access);
				openTask.RunSynchronously();

				var stream = openTask.Result;
				return new PortableFile(CurrentFileSystem, path, stream, readOnly);
			} catch (IOException) {
				throw;
			} catch (Exception ex) {
				throw new IOException(String.Format("Error while opening file '{0}'.", path), ex);
			}
		}

		private IFolder GetParentFolder(string path, out string fileName) {
			fileName = path;
			IFolder parentFolder;
			var sepIndex = path.LastIndexOf(PortablePath.DirectorySeparatorChar);
			if (sepIndex == -1) {
				parentFolder = CurrentFileSystem.LocalStorage;
			} else {
				var folderPath = path.Substring(0, sepIndex);
				fileName = path.Substring(sepIndex + 1);

				var getFolderTask = CurrentFileSystem.GetFolderFromPathAsync(folderPath);
				getFolderTask.RunSynchronously();
				parentFolder = getFolderTask.Result;
			}

			return parentFolder;
		}

		public IFile CreateFile(string path) {
			try {
				var task = CurrentFileSystem.GetFileFromPathAsync(path);
				task.RunSynchronously();

				var file = task.Result;
				if (file != null)
					throw new ArgumentException(String.Format("The file '{0}' already exist in the current file-system.", path));

				string fileName;
				var parentFolder = GetParentFolder(path, out fileName);

				if (parentFolder == null)
					throw new IOException(String.Format("The parent folder of the file '{0}' does not exist.", path));

				var createTask = parentFolder.CreateFileAsync(fileName, CreationCollisionOption.FailIfExists);
				createTask.RunSynchronously();

				file = createTask.Result;
				if (file == null)
					throw new IOException(String.Format("Could not create file '{0}'.", path));

				var openTask = file.OpenAsync(FileAccess.ReadAndWrite);
				openTask.RunSynchronously();

				var stream = openTask.Result;
				return new PortableFile(CurrentFileSystem, path, stream, false);
			} catch (IOException) {
				throw;
			} catch (Exception ex) {
				throw new IOException(String.Format("Error while creating file '{0}'.", path), ex);
			}
		}

		public bool DeleteFile(string path) {
			try {
				var getFileTask = CurrentFileSystem.GetFileFromPathAsync(path);
				getFileTask.RunSynchronously();

				var file = getFileTask.Result;
				if (file == null)
					return false;

				var deleteTask = file.DeleteAsync();
				deleteTask.RunSynchronously();
				return true;
			} catch (IOException) {
				throw;
			} catch (Exception ex) {
				throw new IOException(String.Format("Error while deleting file '{0}'.", path), ex);
			}
		}

		public string CombinePath(string path1, string path2) {
			return PortablePath.Combine(path1, path2);
		}

		public bool RenameFile(string sourcePath, string destPath) {
			throw new NotImplementedException();
		}

		public bool DirectoryExists(string path) {
			try {
				var getFolderTask = CurrentFileSystem.GetFolderFromPathAsync(path);
				getFolderTask.RunSynchronously();
				return getFolderTask.Result != null;
			} catch (IOException) {
				throw;
			} catch (Exception ex) {
				throw new IOException(String.Format("Error while getting the folder '{0}'.", path), ex);
			}
		}

		public void CreateDirectory(string path) {
			try {
				string folder;
				var parentFolder = GetParentFolder(path, out folder);
				if (parentFolder == null)
					throw new IOException(String.Format("The parent folder of '{0}' was not found.", path));

				var createTask = parentFolder.CreateFolderAsync(folder, CreationCollisionOption.FailIfExists);
				createTask.RunSynchronously();

				var newFolder = createTask.Result;
				if (newFolder == null)
					throw new IOException(String.Format("The folder '{0}' was not created.", path));
			} catch (IOException) {
				throw;
			} catch (Exception ex) {
				throw new IOException(String.Format("Error while creating folder '{0}'.", path));
			}
		}

		public long GetFileSize(string path) {
			try {
				string fileName;
				var folder = GetParentFolder(path, out fileName);

				if (folder == null)
					throw new IOException(String.Format("Could not find the parent folder of '{0}'.", path));

				var getFile = folder.GetFileAsync(fileName);
				getFile.RunSynchronously();

				var file = getFile.Result;

				Stream stream = null;

				try {
					var openTask = file.OpenAsync(FileAccess.Read);
					openTask.RunSynchronously();
					stream = openTask.Result;

					return stream.Length;
				} finally {
					if (stream != null)
						stream.Dispose();
				}
			} catch (IOException) {
				throw;
			} catch (Exception ex) {
				throw new IOException(String.Format("Could not determine the file size of file '{0}'.", path), ex);
			}
		}
	}
}
