using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Deveel.Data.Store.Journaled {
	[DebuggerDisplay("{TableName}")]
	public sealed class JournaledStore : StoreBase {
		internal JournaledStore(BufferManager bufferManager, string tableName, bool isReadOnly) 
			: base(isReadOnly) {
			TableName = tableName;
			BufferManager = bufferManager;
			Resource = bufferManager.CreateResource(tableName);
		}

		private BufferManager BufferManager { get; set; }

		private string TableName { get; set; }

		private IJournaledResource Resource { get; set; }

		public bool Exists {
			get { return Resource.Exists; }
		}

		protected override long DataAreaEndOffset {
			get { return Resource.Size; }
		}

		protected override void SetDataAreaSize(long length) {
			Resource.SetSize(length);
		}

		public override void Lock() {
			try {
				BufferManager.Lock();
			} catch (ThreadInterruptedException) {
				throw new IOException("The thread was interrupted");
			}
		}

		public override void Unlock() {
			try {
				BufferManager.Release();
			} catch (ThreadInterruptedException) {
				throw new IOException("The thread was interrupted");
			}
		}

		public void Delete() {
			Resource.Delete();
		}

		protected override void OpenStore(bool readOnly) {
			Resource.Open(readOnly);
		}

		protected override void CloseStore() {
			BufferManager.CloseStore(Resource);
		}

		protected override int Read(long offset, byte[] buffer, int index, int length) {
			return BufferManager.Read(Resource, offset, buffer, index, length);
		}

		protected override void Write(long offset, byte[] buffer, int index, int length) {
			BufferManager.Write(Resource, offset, buffer, index, length);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (Resource != null)
					Resource.Dispose();
			}

			Resource = null;
			base.Dispose(disposing);
		}
	}
}
