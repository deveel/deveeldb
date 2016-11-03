using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public class ObjectCache<TObject> : IDisposable {
		private Dictionary<ObjectName, TObject> byName;
		private Dictionary<ObjectName, ObjectName> nameMap;

		private Dictionary<ObjectName, int> offsets;
		private Dictionary<int, ObjectName> reverseOffsets;

		public ObjectCache() {
			byName = new Dictionary<ObjectName, TObject>(ObjectNameEqualityComparer.Ordinal);
			nameMap = new Dictionary<ObjectName, ObjectName>(ObjectNameEqualityComparer.CaseInsensitive);
			offsets = new Dictionary<ObjectName, int>(ObjectNameEqualityComparer.Ordinal);
			reverseOffsets = new Dictionary<int, ObjectName>();
		}

		~ObjectCache() {
			Dispose(false);
		}

		public IEnumerable<ObjectName> Names {
			get { return byName.Keys; }
		}

		public IEnumerable<TObject> Values {
			get { return byName.Values; }
		}

		public bool ContainsKey(ObjectName name) {
			return byName.ContainsKey(name);
		}

		public void Set(ObjectName name, TObject obj) {
			byName[name] = obj;
			nameMap[name] = name;
			offsets.Clear();
		}

		public bool Remove(ObjectName name) {
			if (!byName.Remove(name))
				return false;

			nameMap.Remove(name);
			offsets.Remove(name);
			return true;
		}

		public bool TryGet(ObjectName name, out TObject obj) {
			return byName.TryGetValue(name, out obj);
		}

		public ObjectName ResolveName(ObjectName name, bool ignoreCase) {
			ObjectName result = null;
			if (ignoreCase && !nameMap.TryGetValue(name, out result)) {
				return null;
			} else if (byName.ContainsKey(name)) {
				return name;
			}

			return result;
		}

		public int Offset(ObjectName name, Func<ObjectName, int> finder) {
			int offset;
			if (!offsets.TryGetValue(name, out offset)) {
				offset = finder(name);
				offsets[name] = offset;

				if (offset > 0)
					reverseOffsets[offset] = name;
			}

			return offset;
		}

		public ObjectName NameAt(int offset, Func<int, ObjectName> finder) {
			ObjectName name;
			if (!reverseOffsets.TryGetValue(offset, out name)) {
				name = finder(offset);

				if (name != null) {
					offsets[name] = offset;
					reverseOffsets[offset] = name;
				}
			}

			return name;
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (byName != null)
					byName.Clear();
				if (nameMap != null)
					nameMap.Clear();
				if (offsets != null)
					offsets.Clear();
				if (reverseOffsets != null)
					reverseOffsets.Clear();
			}

			reverseOffsets = null;
			offsets = null;
			nameMap = null;
			byName = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public void Clear() {
			byName.Clear();
			nameMap.Clear();
			offsets.Clear();
		}
	}
}
