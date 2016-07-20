using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Objects {
	[Serializable]
	public sealed class SqlUserObject : ISqlObject, ISerializable, IDisposable {
		private Dictionary<string, ISqlObject> values;

		private SqlUserObject(bool isNull) {
			IsNull = isNull;
		}

		private SqlUserObject(SerializationInfo info, StreamingContext context) {
			var isNull = info.GetBoolean("IsNull");

			if (!isNull) {
				var valueCount = info.GetInt32("ValueCount");
				var memberNames = (string[]) info.GetValue("MemberNames", typeof(string));
				var memberValues = (ISqlObject[]) info.GetValue("MemberValues", typeof(ISqlObject[]));

				if (memberNames == null || memberNames.Length == 0 || memberNames.Length != valueCount)
					throw new SerializationException("Invalid number of member names");
				if (memberValues == null || memberValues.Length == 0 || memberValues.Length != valueCount)
					throw new SerializationException("Invalid number of member values");

				values = new Dictionary<string, ISqlObject>(valueCount);

				for (int i = 0; i < valueCount; i++) {
					values[memberNames[i]] = memberValues[i];
				}
			} else {
				values = new Dictionary<string, ISqlObject>(0);
			}

			IsNull = isNull;
		}

		internal SqlUserObject(IDictionary<string, ISqlObject> values)
			: this(false) {
			this.values = new Dictionary<string, ISqlObject>(values);
		}

		~SqlUserObject() {
			Dispose(false);
		}

		public static readonly SqlUserObject Null = new SqlUserObject(true);

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException("Comparison between objects is not supported (yet).");
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException("Comparison between objects is not supported (yet).");
		}

		public bool IsNull { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (values != null) {
					foreach (var pair in values) {
						var value = pair.Value;
						if (value is IDisposable)
							((IDisposable)value).Dispose();
					}

					values.Clear();
				}
			}

			values = null;
		}

		public ISqlObject GetValue(string memberName) {
			if (String.IsNullOrEmpty(memberName))
				throw new ArgumentNullException("memberName");

			if (IsNull)
				throw new InvalidOperationException("The object is null.");

			ISqlObject value;
			if (!values.TryGetValue(memberName, out value))
				return SqlNull.Value;

			return value;
		}

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			if (IsNull) {
				info.AddValue("IsNull", true);
			} else {
				info.AddValue("IsNull", false);

				var count = values.Count;
				var names = values.Keys.ToArray();
				var valuesArray = values.Values.ToArray();

				info.AddValue("ValueCount", count);
				info.AddValue("MemberNames", names, typeof(string[]));
				info.AddValue("MemberValues", valuesArray, typeof(ISqlObject[]));
			}
		}
	}
}
