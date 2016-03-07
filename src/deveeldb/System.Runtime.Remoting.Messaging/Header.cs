using System;

namespace System.Runtime.Remoting.Messaging {
	[Serializable]
	public class Header {
		public Header(string name, object value) :
			this(name, value, true) {
		}

		public Header(string name, object value, bool mustUnderstand) :
			this(name, value, mustUnderstand, null) {
		}

		public Header(string name, object value, bool mustUnderstand, string headerNamespace) {
			Name = name;
			Value = value;
			MustUnderstand = mustUnderstand;
			HeaderNamespace = headerNamespace;
		}


		public string HeaderNamespace { get; private set; }

		public bool MustUnderstand { get; private set; }

		public string Name { get; private set; }

		public object Value { get; private set; }
	}
}
