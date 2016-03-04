using System;
using System.Collections;
using System.Collections.Generic;

namespace System.Runtime.Serialization {
	public class ObjectIDGenerator {
		private readonly IDictionary<object, long> table;
		private long currentId;

		public ObjectIDGenerator() {
			table = new Dictionary<object, long>(new ObjectComparer());
			currentId = 1;
		}

		public virtual long GetId(object obj, out bool firstTime) {
			if (obj == null)
				throw new ArgumentNullException("obj");

			long value;
			if (table.TryGetValue(obj, out value)) {
				firstTime = false;
				return value;
			}

			firstTime = true;
			table.Add(obj, currentId);
			return currentId++;
		}

		public virtual long HasId(object obj, out bool firstTime) {
			if (obj == null)
				throw new ArgumentNullException("obj");

			long value;
			if (table.TryGetValue(obj, out value)) {
				firstTime = false;
				return value;
			}

			firstTime = true;
			return 0;
		}

		class ObjectComparer : IEqualityComparer<object> {
			public new bool Equals(object x, object y) {
				if (x is string)
					return x.Equals(y);

				return x == y;
			}

			public int GetHashCode(object obj) {
				return obj.GetHashCode();
			}
		}
	}
}
