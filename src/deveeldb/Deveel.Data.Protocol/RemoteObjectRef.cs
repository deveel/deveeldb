using System;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Store;

namespace Deveel.Data.Deveel.Data.Protocol {
	public sealed class RemoteObjectRef : IObjectRef, ISqlObject {
		public RemoteObjectRef(ObjectId objectId, long size) {
			ObjectId = objectId;
			Size = size;
		}

		public ObjectId ObjectId { get; private set; }

		public long Size { get; private set; }

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException();
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException();
		}

		bool ISqlObject.IsNull {
			get { return false; }
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}
	}
}
