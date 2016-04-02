using System;
using System.IO;

namespace Deveel.Data.Store.Journaled {
	class NonLoggingResource : ResourceBase {
		public NonLoggingResource(JournaledSystem journaledSystem, long id, string name, IStoreData data) 
			: base(journaledSystem, id, name, data) {
		}

		public override long Size {
			get { return Data.Length; }
		}

		public override bool Exists {
			get { return Data.Exists; }
		}

		public override void Read(long pageNumber, byte[] buffer, int offset) {
			long pagePosition = pageNumber * JournaledSystem.PageSize;
			Data.Read(pagePosition + offset, buffer, offset, JournaledSystem.PageSize);
		}

		public override void Write(long pageNumber, byte[] buffer, int offset, int count) {
			long pagePosition = pageNumber * JournaledSystem.PageSize;
			Data.Write(pagePosition + offset, buffer, offset, count);
		}

		public override void SetSize(long value) {
			Data.SetLength(value);
		}

		public override void Open(bool readOnly) {
			SetReadOnly(readOnly);
			Data.Open(readOnly);
		}

		public override void Close() {
			Data.Close();
		}

		public override void Delete() {
			Data.Delete();
		}

		internal override void PersistOpen(bool readOnly) {
		}

		internal override void PersistClose() {
		}

		internal override void PersistDelete() {
		}

		internal override void PersistSetSize(long newSize) {
		}

		internal override void PersistPageChange(long page, int offset, int count, BinaryReader reader) {
		}

		internal override void Synch() {
		}

		internal override void OnPostRecover() {
		}
	}
}
