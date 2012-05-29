using System;
using System.IO;

namespace Deveel.Data.Control {
	public sealed class XmlConfigFormatter : IConfigFormatter {
		public void LoadFrom(DbConfig config, Stream inputStream) {
			throw new NotImplementedException();
		}

		public void SaveTo(DbConfig config, Stream outputStream) {
			throw new NotImplementedException();
		}
	}
}