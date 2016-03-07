using System;
using System.Collections;
using System.Collections.Generic;

namespace System.Runtime.Serialization {
	public sealed class SerializationEnumerator : IEnumerator<SerializationEntry> {
		private readonly IEnumerator<SerializationEntry> enumerator;

		internal SerializationEnumerator(IEnumerable<SerializationEntry> entries) {
			enumerator = entries.GetEnumerator();
		}

		public bool MoveNext() {
			return enumerator.MoveNext();
		}

		public void Reset() {
			enumerator.Reset();
		}

		public SerializationEntry Current {
			get { return enumerator.Current; }
		}

		object IEnumerator.Current {
			get { return Current; }
		}

		public string Name {
			get { return Current.Name; }
		}

		public Type ObjectType {
			get { return Current.ObjectType; }
		}

		public object Value {
			get { return Current.Value; }
		}

		public void Dispose() {
		}
	}
}
