using System;

namespace Deveel.Data.Store.Journaled {
	public sealed class JournaledFileStore : StoreBase {
		private readonly LoggingBufferManager bufferManager;
		private IJournaledResource resource;

		internal JournaledFileStore(string resourceName, LoggingBufferManager bufferManager, bool readOnly) 
			: base(readOnly) {
			this.bufferManager = bufferManager;
			resource = bufferManager.CreateResource(resourceName);
		}

		protected override long DataAreaEndOffset {
			get { return resource.Size; }
		}

		public void Delete() {
			resource.Delete();
		}

		public bool Exists() {
			return resource.Exists;
		}

		protected override void SetDataAreaSize(long length) {
			resource.SetSize(length);
		}

		public override void Lock() {
			bufferManager.Lock();
		}

		public override void Unlock() {
			bufferManager.Unlock();
		}

		protected override void OpenStore(bool readOnly) {
			resource.Open(readOnly);
		}

		protected override void CloseStore() {
			bufferManager.Close(resource);
		}

		protected override int Read(long offset, byte[] buffer, int index, int length) {
			return bufferManager.ReadFrom(resource, offset, buffer, index, length);
		}

		protected override void Write(long offset, byte[] buffer, int index, int length) {
			bufferManager.WriteTo(resource, offset, buffer, index, length);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (resource != null)
					resource.Dispose();
			}

			resource = null;

			base.Dispose(disposing);
		}
	}
}
