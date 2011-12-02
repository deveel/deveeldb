using System;
using System.IO;

namespace Deveel.Data.Commands {
	[Serializable]
	class EncodingMismatchException : IOException {
		private readonly string _encoding;

		public EncodingMismatchException(String encoding)
			: base("file encoding Mismatch Exception; got " + encoding) {
			_encoding = encoding;
		}

		public String Encoding {
			get { return _encoding; }
		}
	}
}