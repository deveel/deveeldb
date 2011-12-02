using System;

namespace Deveel.Data.Client {
	internal class LargeObjectRef {
		private readonly ReferenceType type;
		private readonly long id;
		private readonly long size;

		public LargeObjectRef(long id, ReferenceType type, long size) {
			this.id = id;
			this.size = size;
			this.type = type;
		}

		public long Size {
			get { return size; }
		}

		public long Id {
			get { return id; }
		}

		public ReferenceType Type {
			get { return type; }
		}
	}
}