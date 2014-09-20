// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Store {
	/// <summary>
	/// An implementation of <see cref="IStoreDataAccessor"/> that scatters the 
	/// addressible data resource across multiple files in the file system.
	/// </summary>
	/// <remarks>
	/// When one store data resource reaches a certain threshold size, the content 
	/// <i>flows</i> over to the next file.
	/// </remarks>
	class ScatteringStoreDataAccessor : IStoreDataAccessor {
		/// <summary>
		///The path of this store in the file system. 
		/// </summary>
		private readonly string path;

		/// <summary>
		/// The name of the file in the file system minus the extension.
		/// </summary>
		private readonly string fileName;

		/// <summary>
		/// The extension of the first file in the sliced set.
		/// </summary>
		private readonly string firstExt;

		/// <summary>
		/// The maximum size a file slice can grow too before a new slice is created. 
		/// </summary>
		private readonly long maxSliceSize;

		/// <summary>
		/// The list of RandomAccessFile objects for each file that represents a
		/// slice of the store.  (FileSlice objects)
		/// </summary>
		private List<FileSlice> sliceList;

		/// <summary>
		/// The current actual physical size of the store data on disk.
		/// </summary>
		private long trueFileLength;

		/// <summary>
		/// A lock when modifying the true_data_size, and slice_list. 
		/// </summary>
		private readonly object l = new Object();

		/// <summary>
		/// Set when the store is openned.
		/// </summary>
		private bool open;

		/// <summary>
		/// Constructs the store data accessor.
		/// </summary>
		/// <param name="path"></param>
		/// <param name="fileName"></param>
		/// <param name="firstExt"></param>
		/// <param name="maxSliceSize"></param>
		public ScatteringStoreDataAccessor(string path, string fileName, string firstExt, long maxSliceSize) {
			sliceList = new List<FileSlice>();
			this.path = path;
			this.fileName = fileName;
			this.firstExt = firstExt;
			this.maxSliceSize = maxSliceSize;
		}

		~ScatteringStoreDataAccessor() {
			Dispose(false);
		}

		/// <summary>
		/// Given a file, this will convert to a scattering file store with files 
		/// no larger than the maximum slice size.
		/// </summary>
		/// <param name="f"></param>
		public void ConvertToScatteringStore(string f) {
			const int bufferSize = 65536;

			FileStream src = new FileStream(f, FileMode.OpenOrCreate, FileAccess.ReadWrite);
			long fileSize = new FileInfo(f).Length;
			long currentP = maxSliceSize;
			long toWrite = System.Math.Min(fileSize - currentP, maxSliceSize);
			int writeToPart = 1;

			byte[] copy_buffer = new byte[bufferSize];

			while (toWrite > 0) {
				src.Seek(currentP, SeekOrigin.Begin);

				string toF = SlicePartFile(writeToPart);
				if (File.Exists(toF))
					throw new IOException("Copy error, slice already exists.");

				FileStream destStream = new FileStream(toF, FileMode.OpenOrCreate, FileAccess.Write);

				while (toWrite > 0) {
					int sizeToCopy = (int)System.Math.Min(bufferSize, toWrite);

					src.Read(copy_buffer, 0, sizeToCopy);
					destStream.Write(copy_buffer, 0, sizeToCopy);

					currentP += sizeToCopy;
					toWrite -= sizeToCopy;
				}

				destStream.Flush();
				destStream.Close();

				toWrite = System.Math.Min(fileSize - currentP, maxSliceSize);
				++writeToPart;
			}

			// Truncate the source file
			if (fileSize > maxSliceSize) {
				src.Seek(0, SeekOrigin.Begin);
				src.SetLength(maxSliceSize);
			}
			src.Close();

		}

		/// <summary>
		/// Given an index value, this will returns the path to the nth slice 
		/// in the file system.
		/// </summary>
		/// <param name="i"></param>
		/// <remarks>
		/// For example, given '4' will return [file name].004, given 1004 will return 
		/// [file name].1004, etc.
		/// </remarks>
		/// <returns></returns>
		private string SlicePartFile(int i) {
			if (i == 0)
				return Path.Combine(path, fileName + "." + firstExt);

			StringBuilder fn = new StringBuilder();
			fn.Append(fileName);
			fn.Append(".");
			if (i < 10) {
				fn.Append("00");
			} else if (i < 100) {
				fn.Append("0");
			}
			fn.Append(i);
			return Path.Combine(path, fn.ToString());
		}

		/// <summary>
		/// Counts the number of files in the file store that represent this store.
		/// </summary>
		private int StoreFilesCount {
			get {
				int i = 0;
				string f = SlicePartFile (i);
				while (File.Exists (f)) {
					++i;
					f = SlicePartFile (i);
				}
				return i;
			}
		}

		/// <summary>
		/// Creates a <see cref="IStoreDataAccessor"/> object for accessing a given slice.
		/// </summary>
		/// <param name="file"></param>
		/// <returns>
		/// </returns>
		private IStoreDataAccessor CreateSliceDataAccessor(string file) {
			// Currently we only support an IOStoreDataAccessor object.
			return new IOStoreDataAccessor(file);
		}

		/// <summary>
		/// Discovers the size of the data resource (doesn't require the file 
		/// to be open). 
		/// </summary>
		/// <returns></returns>
		private long DiscoverSize() {
			long runningTotal = 0;

			lock (l) {
				// Does the file exist?
				int i = 0;
				string f = SlicePartFile(i);
				while (File.Exists(f)) {
					runningTotal += CreateSliceDataAccessor(f).Size;

					++i;
					f = SlicePartFile(i);
				}
			}

			return runningTotal;
		}

		// ---------- Implemented from IStoreDataAccessor ----------

		public void Open(bool readOnly) {
			lock (l) {
				sliceList = new List<FileSlice>();

				// Does the file exist?
				string f = SlicePartFile(0);
				bool openExisting = File.Exists(f);

				// If the file already exceeds the threshold and there isn't a secondary
				// file then we need to convert the file.
				if (openExisting && f.Length > maxSliceSize) {
					string f2 = SlicePartFile(1);
					if (File.Exists(f2))
						throw new IOException("File length exceeds maximum slice size setting.");

					// We need to scatter the file.
					if (readOnly)
						throw new IOException("Unable to convert to a scattered store because Read-only.");
						
					ConvertToScatteringStore(f);
				}

				// Setup the first file slice
				FileSlice slice = new FileSlice();
				slice.data = CreateSliceDataAccessor(f);
				slice.data.Open(readOnly);

				sliceList.Add(slice);
				long runningLength = slice.data.Size;

				// If we are opening a store that exists already, there may be other
				// slices we need to setup.
				if (openExisting) {
					int i = 1;
					string slicePart = SlicePartFile(i);
					while (File.Exists(slicePart)) {
						// Create the new slice information for this part of the file.
						slice = new FileSlice();
						slice.data = CreateSliceDataAccessor(slicePart);
						slice.data.Open(readOnly);

						sliceList.Add(slice);
						runningLength += slice.data.Size;

						++i;
						slicePart = SlicePartFile(i);
					}
				}

				trueFileLength = runningLength;

				open = true;
			}
		}

		public void Close() {
			lock (l) {
				foreach (FileSlice slice in sliceList) {
					slice.data.Close();
				}
				sliceList = null;
				open = false;
			}
		}

		public bool Delete() {
			// The number of files
			int countFiles = StoreFilesCount;
			// Delete each file from back to front
			for (int i = countFiles - 1; i >= 0; --i) {
				string f = SlicePartFile(i);
				bool deleteSuccess = CreateSliceDataAccessor(f).Delete();
				if (!deleteSuccess)
					return false;
			}
			return true;
		}

		public bool Exists {
			get { return File.Exists(SlicePartFile(0)); }
		}


		public int Read(long position, byte[] buf, int off, int len) {
			// Reads the array (potentially across multiple slices).
			int count = 0;
			while (len > 0) {
				int fileI = (int)(position / maxSliceSize);
				long fileP = (position % maxSliceSize);
				int fileLen = (int)System.Math.Min((long)len, maxSliceSize - fileP);

				FileSlice slice;
				lock (l) {
					// Return if out of bounds.
					if (fileI < 0 || fileI >= sliceList.Count) {
						// Error if not open
						if (!open)
							throw new IOException("Store not open.");

						return 0;
					}
					slice = sliceList[fileI];
				}

				int readCount =slice.data.Read(fileP, buf, off, fileLen);
				if (readCount == 0)
					break;

				position += readCount;
				off += readCount;
				len -= readCount;
				count += readCount;
			}

			return count;
		}

		public void Write(long position, byte[] buf, int off, int len) {
			// Writes the array (potentially across multiple slices).
			while (len > 0) {
				int fileI = (int)(position / maxSliceSize);
				long fileP = (position % maxSliceSize);
				int fileLen = (int)System.Math.Min((long)len, maxSliceSize - fileP);

				FileSlice slice;
				lock (l) {
					// Return if out of bounds.
					if (fileI < 0 || fileI >= sliceList.Count) {
						if (!open)
							throw new IOException("Store not open.");

						return;
					}
					slice = sliceList[fileI];
				}
				slice.data.Write(fileP, buf, off, fileLen);

				position += fileLen;
				off += fileLen;
				len -= fileLen;
			}
		}

		public void SetSize(long newSize) {
			lock (l) {
				// The size we need to grow the data area
				long totalSizeToGrow = newSize - trueFileLength;
				// Assert that we aren't shrinking the data area size.
				if (totalSizeToGrow < 0) {
					throw new IOException("Unable to make the data area size " +
										  "smaller for this type of store.");
				}

				while (totalSizeToGrow > 0) {
					// Grow the last slice by this size
					int last = sliceList.Count - 1;
					FileSlice slice = sliceList[last];
					long oldSliceLength = slice.data.Size;
					long toGrow = System.Math.Min(totalSizeToGrow, (maxSliceSize - oldSliceLength));

					// Flush the buffer and set the length of the file
					slice.data.SetSize(oldSliceLength + toGrow);
					// Synchronize the file change.  XP appears to defer a file size change
					// and it can result in errors if the JVM is terminated.
					slice.data.Synch();

					totalSizeToGrow -= toGrow;
					// Create a new empty slice if we need to extend the data area
					if (totalSizeToGrow > 0) {
						string sliceFile = SlicePartFile(last + 1);

						slice = new FileSlice();
						slice.data = CreateSliceDataAccessor(sliceFile);
						slice.data.Open(false);

						sliceList.Add(slice);
					}
				}
				trueFileLength = newSize;
			}

		}

		public long Size {
			get {
				lock (l) {
					return open ? trueFileLength : DiscoverSize();
				}
			}
		}

		public void Synch() {
			lock (l) {
				foreach (FileSlice slice in sliceList)
					slice.data.Synch();
			}
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// An object that contains information about a file slice.
		/// </summary>
		/// <remarks>
		/// The information includes the name of the file, the <see cref="FileStream"/>
		/// that represents the slice, and the size of the file.
		/// </remarks>
		private sealed class FileSlice {
			internal IStoreDataAccessor data;

		}

		private void Dispose(bool disposing) {
			if (disposing) {
				foreach (FileSlice slice in sliceList) {
					slice.data.Dispose();
				}
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}