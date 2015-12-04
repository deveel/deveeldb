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
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Store {
	public sealed class ScatteringFileStoreData : IStoreData {
		private readonly object objectLock = new object();
		private List<FileStoreData> fileSlices;
		private long trueFileLength;
		
		public ScatteringFileStoreData(IFileSystem fileSystem, string basePath, string fileName, string fileExtention, int maxFileSlice) {
			if (fileSystem == null)
				throw new ArgumentNullException("fileSystem");

			FileSystem = fileSystem;
			MaxFileSlice = maxFileSlice;
			FileExtention = fileExtention;
			FileName = fileName;
			BasePath = basePath;
			fileSlices = new List<FileStoreData>();
		}

		~ScatteringFileStoreData() {
			Dispose(false);
		}

		public string BasePath { get; private set; }

		public string FileName { get; private set; }

		public string FileExtention { get; private set; }

		public int MaxFileSlice { get; private set; }

		public IFileSystem FileSystem { get; private set; }

		public int FileCount {
			get {
				int i = 0;
				string f = SliceFileName(i);
				while (FileSystem.FileExists(f)) {
					++i;
					f = SliceFileName(i);
				}
				return i;
			}
		}

		public bool IsOpen { get; private set; }

		private string SliceFileName(int i) {
			if (i == 0)
				return FileSystem.CombinePath(BasePath, String.Format("{0}.{1}", FileName, FileExtention));

			var fn = new StringBuilder();
			fn.Append(FileName);
			fn.Append(".");
			if (i < 10) {
				fn.Append("00");
			} else if (i < 100) {
				fn.Append("0");
			}
			fn.Append(i);
			return Path.Combine(BasePath, fn.ToString());
		}

		private long DiscoverSize() {
			long runningTotal = 0;

			lock (objectLock) {
				// Does the file exist?
				int i = 0;
				string f = SliceFileName(i);
				while (FileSystem.FileExists(f)) {
					var fileLength = FileSystem.GetFileSize(f);

					runningTotal += fileLength;

					++i;
					f = SliceFileName(i);
				}
			}

			return runningTotal;
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				foreach (var slice in fileSlices) {
					if (slice != null)
						slice.Dispose();
				}

				fileSlices.Clear();
				fileSlices = null;
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public bool Exists {
			get { return FileSystem.FileExists(SliceFileName(0)); }
		}

		public long Length {
			get {
				lock (objectLock) {
					return IsOpen ? trueFileLength : DiscoverSize();
				}
			}
		}

		public bool IsReadOnly { get; private set; }

		public bool Delete() {
			// The number of files
			int countFiles = FileCount;
			// Delete each file from back to front
			for (int i = countFiles - 1; i >= 0; --i) {
				string f = SliceFileName(i);
				bool deleteSuccess = new FileStoreData(f).Delete();
				if (!deleteSuccess)
					return false;
			}
			return true;
		}

		public void Open(bool readOnly) {
			lock (objectLock) {
				// Does the file exist?
				string f = SliceFileName(0);
				bool openExisting = FileSystem.FileExists(f);

				// If the file already exceeds the threshold and there isn't a secondary
				// file then we need to convert the file.
				if (openExisting && f.Length > MaxFileSlice) {
					string f2 = SliceFileName(1);
					if (FileSystem.FileExists(f2))
						throw new IOException("File length exceeds maximum slice size setting.");

					// We need to scatter the file.
					if (readOnly)
						throw new IOException("Unable to convert to a scattered store because Read-only.");						
				}

				// Setup the first file slice
				var slice = new FileStoreData(f);
				slice.Open(readOnly);

				fileSlices.Add(slice);
				long runningLength = slice.Length;

				// If we are opening a store that exists already, there may be other
				// slices we need to setup.
				if (openExisting) {
					int i = 1;
					string slicePart = SliceFileName(i);
					while (FileSystem.FileExists(slicePart)) {
						// Create the new slice information for this part of the file.
						slice = new FileStoreData(slicePart);
						slice.Open(readOnly);

						fileSlices.Add(slice);
						runningLength += slice.Length;

						++i;
						slicePart = SliceFileName(i);
					}
				}

				trueFileLength = runningLength;
				IsOpen = true;
				IsReadOnly = readOnly;
			}

		}

		public void Close() {
			lock (objectLock) {
				foreach (var slice in fileSlices) {
					slice.Close();
				}
			}
		}

		public int Read(long position, byte[] buffer, int offset, int length) {
			// Reads the array (potentially across multiple slices).
			int count = 0;
			while (length > 0) {
				int fileI = (int)(position / MaxFileSlice);
				long fileP = (position % MaxFileSlice);
				int fileLen = (int)System.Math.Min((long)length, MaxFileSlice - fileP);

				FileStoreData slice;
				lock (objectLock) {
					// Return if out of bounds.
					if (fileI < 0 || fileI >= fileSlices.Count) {
						// Error if not open
						if (!IsOpen)
							throw new IOException("Store not open.");

						return 0;
					}
					slice = fileSlices[fileI];
				}

				int readCount =slice.Read(fileP, buffer, offset, fileLen);
				if (readCount == 0)
					break;

				position += readCount;
				offset += readCount;
				length -= readCount;
				count += readCount;
			}

			return count;
		}

		public void Write(long position, byte[] buffer, int offset, int length) {
			// Writes the array (potentially across multiple slices).
			while (length > 0) {
				var fileI = (int)(position / MaxFileSlice);
				var fileP = (position % MaxFileSlice);
				var fileLen = (int)System.Math.Min((long)length, MaxFileSlice - fileP);

				FileStoreData slice;
				lock (objectLock) {
					// Return if out of bounds.
					if (fileI < 0 || fileI >= fileSlices.Count) {
						if (!IsOpen)
							throw new IOException("Store not open.");

						return;
					}
					slice = fileSlices[fileI];
				}

				slice.Write(fileP, buffer, offset, fileLen);

				position += fileLen;
				offset += fileLen;
				length -= fileLen;
			}
		}

		public void Flush() {
			lock (objectLock) {
				foreach (var slice in fileSlices) {
					slice.Flush();
				}
			}
		}

		public void SetLength(long value) {
			lock (objectLock) {
				// The size we need to grow the data area
				long totalSizeToGrow = value - trueFileLength;
				// Assert that we aren't shrinking the data area size.
				if (totalSizeToGrow < 0) {
					throw new IOException("Unable to make the data area size " +
										  "smaller for this type of store.");
				}

				while (totalSizeToGrow > 0) {
					// Grow the last slice by this size
					int last = fileSlices.Count - 1;
					var slice = fileSlices[last];
					long oldSliceLength = slice.Length;
					long toGrow = System.Math.Min(totalSizeToGrow, (MaxFileSlice - oldSliceLength));

					// Flush the buffer and set the length of the file
					slice.SetLength(oldSliceLength + toGrow);
					slice.Flush();

					totalSizeToGrow -= toGrow;
					// Create a new empty slice if we need to extend the data area
					if (totalSizeToGrow > 0) {
						string sliceFile = SliceFileName(last + 1);

						slice = new FileStoreData(sliceFile);
						slice.Open(false);

						fileSlices.Add(slice);
					}
				}
				trueFileLength = value;
			}
		}
	}
}