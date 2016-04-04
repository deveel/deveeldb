using System;
using System.IO;

namespace Deveel.Data.Store {
	public sealed class PortableFile : IFile {
		private Stream fileStream;

		internal PortableFile(PCLStorage.IFileSystem fileSystem, string fileName, Stream fileStream, bool readOnly) {
			if (String.IsNullOrEmpty(fileName))
				throw new ArgumentNullException("fileName");

			if (fileStream == null)
				throw new ArgumentNullException("fileStream");

			this.fileStream = fileStream;
			FileSystem = fileSystem;
			FileName = fileName;
			IsReadOnly = readOnly;
		}

		~PortableFile() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (fileStream != null)
					fileStream.Dispose();
			}

			fileStream = null;
		}

		private PCLStorage.IFileSystem FileSystem { get; set; }

		public string FileName { get; private set; }

		public bool IsReadOnly { get; private set; }

		public long Position {
			get { return fileStream.Position; }
		}

		public long Length {
			get { return fileStream.Length; }
		}

		public bool Exists {
			get {
				var getFileTask = FileSystem.GetFileFromPathAsync(FileName);
				getFileTask.RunSynchronously();
				return getFileTask.Result != null;
			}
		}

		public long Seek(long offset, SeekOrigin origin) {
			return fileStream.Seek(offset, origin);
		}

		public void SetLength(long value) {
			fileStream.SetLength(value);
		}

		public int Read(byte[] buffer, int offset, int length) {
			return fileStream.Read(buffer, offset, length);
		}

		public void Write(byte[] buffer, int offset, int length) {
			fileStream.Write(buffer, offset, length);
		}

		public void Flush(bool writeThrough) {
			fileStream.Flush();
		}

		public void Close() {
		}

		public void Delete() {
			try {
				var getFile = FileSystem.GetFileFromPathAsync(FileName);
				getFile.RunSynchronously();

				var file = getFile.Result;
				if (file == null)
					throw new IOException(String.Format("File '{0}' does not exist.", FileName));

				var deleteTask = file.DeleteAsync();
				deleteTask.RunSynchronously();
			} catch (IOException) {
				throw;
			} catch (Exception ex) {
				throw new IOException(String.Format("Error deleting file '{0}'.", FileName), ex);
			}
		}
	}
}
