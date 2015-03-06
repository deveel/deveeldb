// 
//  Copyright 2010-2015 Deveel
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
using System.IO;

namespace Deveel.Data.Store {
	public sealed class FileStoreData : IStoreData {
		private FileStream fileStream;
		private readonly object objectLock = new object();

		public const int BufferSize = 1024*2;

		public FileStoreData(string filePath) {
			if (String.IsNullOrEmpty(filePath))
				throw new ArgumentNullException("filePath");

			FilePath = filePath;
			IsOpen = false;
		}

		~FileStoreData() {
			Dispose(false);
		}

		public string FilePath { get; private set; }

		public bool IsOpen { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (IsOpen)
					Close();

				if (fileStream != null)
					fileStream.Dispose();
			}
		}

		public bool Exists {
			get {
				lock (objectLock) {
					return File.Exists(FilePath);
				}
			}
		}

		public long Length {
			get { return IsOpen ? fileStream.Length : new FileInfo(FilePath).Length; }
		}

		public bool IsReadOnly { get; private set; }

		public bool Delete() {
			if (IsOpen)
				return false;

			try {
				File.Delete(FilePath);
				return !Exists;
			} catch (Exception) {
				return false;
			}
		}

		public void Open(bool readOnly) {
			lock (objectLock) {
				var options = FileOptions.WriteThrough | FileOptions.Encrypted;

				fileStream = new FileStream(FilePath, readOnly ? FileMode.Open : FileMode.OpenOrCreate,
					readOnly ? FileAccess.Read : FileAccess.ReadWrite, FileShare.None, BufferSize, options);
				IsOpen = true;
				IsReadOnly = readOnly;
			}
		}

		public void Close() {
			lock (objectLock) {
				try {
					fileStream.Close();
				} finally {
					IsOpen = false;
				}
			}
		}

		public int Read(long position, byte[] buffer, int offset, int length) {
			// Make sure we don't Read past the end
			lock (objectLock) {
				length = System.Math.Max(0, System.Math.Min(length, (int)(Length - position)));
				int count = 0;
				if (position < Length) {
					fileStream.Seek(position, SeekOrigin.Begin);
					count = fileStream.Read(buffer, offset, length);
				}
				return count;
			}
		}

		public void Write(long position, byte[] buffer, int offset, int length) {
			if (IsReadOnly)
				throw new IOException();

			// Make sure we don't Write past the end
			lock (objectLock) {
				length = System.Math.Max(0, System.Math.Min(length, (int)(Length - position)));
				if (position < Length) {
					fileStream.Seek(position, SeekOrigin.Begin);
					fileStream.Write(buffer, offset, length);
				}
			}
		}

		public void Flush() {
			try {
				fileStream.Flush();
			} catch (IOException) {
				// There isn't much we can do about this exception.  By itself it
				// doesn't indicate a terminating error so it's a good idea to ignore
				// it.  Should it be silently ignored?
			}
		}

		public void SetLength(long value) {
			lock (objectLock) {
				fileStream.SetLength(value);
			}
		}
	}
}