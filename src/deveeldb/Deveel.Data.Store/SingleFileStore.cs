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
using System.Diagnostics;
using System.IO;

using Deveel.Data.Util;

namespace Deveel.Data.Store {
	[DebuggerDisplay("[{Id}] {Name}")]
	public sealed class SingleFileStore : StoreBase {
		private SingleFileStoreSystem system;
		private Stream dataStream;

		// TODO: use this counter to prevent disposing if referenced
		private int lockCount;

		internal SingleFileStore(SingleFileStoreSystem system, string name, int id)
			: base(system.IsReadOnly) {
			this.system = system;
			Name = name;
			Id = id;
		}

		public string Name { get; private set; }

		public int Id { get; private set; }

		public bool IsOpen { get; private set; }

		private object CheckPointLock {
			get { return system.CheckPointLock; }
		}

		protected override long DataAreaEndOffset {
			get { return DataLength; }
		}

		public long DataLength {
			get {
				return dataStream == null ? 0 : dataStream.Length;
			}
		}

		protected override void SetDataAreaSize(long length) {
			lock (CheckPointLock) {
				if (dataStream != null)
					dataStream.SetLength(length);
			}
		}

		internal void Create(int bufferSize) {
			lock (CheckPointLock) {
				dataStream = new MemoryStream(bufferSize);
				IsOpen = true;
			}
		}

		public override void Lock() {
			lockCount++;
		}

		public override void Unlock() {
			lockCount--;
		}

		//public override void CheckPoint() {
		//	throw new NotImplementedException();
		//}

		protected override void OpenStore(bool readOnly) {
			lock (CheckPointLock) {
				dataStream = new MemoryStream();

				var inputStream = system.LoadStoreData(Id);
				if (inputStream != null) {
					inputStream.Seek(0, SeekOrigin.Begin);
					inputStream.CopyTo(dataStream);
				}

				IsOpen = true;
			}
		}

		protected override void CloseStore() {
			lock (CheckPointLock) {
				if (lockCount == 0)
					IsOpen = false;
			}
		}

		protected override int Read(long offset, byte[] buffer, int index, int length) {
			lock (CheckPointLock) {
				if (dataStream == null || !IsOpen)
					return 0;

				dataStream.Seek(offset, SeekOrigin.Begin);
				return dataStream.Read(buffer, index, length);
			}
		}

		protected override void Write(long offset, byte[] buffer, int index, int length) {
			lock (CheckPointLock) {
				if (dataStream != null && IsOpen) {
					dataStream.Seek(offset, SeekOrigin.Begin);
					dataStream.Write(buffer, index, length);
				}
			}
		}

		private bool disposed;

		protected override void Dispose(bool disposing) {
			if (!disposed) {
				base.Dispose(disposing);

				if (disposing) {
					lock (CheckPointLock) {
						if (dataStream != null)
							dataStream.Dispose();
					}
				}

				dataStream = null;
				system = null;
				IsOpen = false;
				disposed = true;
			}
		}

		internal void WriteTo(Stream stream) {
			lock (CheckPointLock) {
				if (dataStream != null) {
					dataStream.Seek(0, SeekOrigin.Begin);
					dataStream.CopyTo(stream);
				}
			}
		}
	}
}
