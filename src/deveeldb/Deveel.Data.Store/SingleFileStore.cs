using System;
using System.IO;

using Deveel.Data.Util;

namespace Deveel.Data.Store {
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
					inputStream.CopyTo(dataStream);
				}

				IsOpen = true;
			}
		}

		protected override void CloseStore() {
			lock (CheckPointLock) {
				if (!IsOpen)
					throw new IOException("The store is already closed.");

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

		protected override void Dispose(bool disposing) {
			if (disposing) {
				lock (CheckPointLock) {
					if (dataStream != null)
						dataStream.Dispose();
				}
			}

			dataStream = null;
			system = null;
			IsOpen = false;
			base.Dispose(disposing);
		}

		internal void WriteTo(Stream stream) {
			lock (CheckPointLock) {
				if (dataStream != null) {
					dataStream.CopyTo(stream);
				}
			}
		}
	}
}
