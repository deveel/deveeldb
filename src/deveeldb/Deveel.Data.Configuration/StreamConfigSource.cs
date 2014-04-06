using System;
using System.IO;

namespace Deveel.Data.Configuration {
	public class StreamConfigSource : IConfigSource {
		public StreamConfigSource(Stream stream) {
			Stream = stream;
		}

		public Stream Stream { get; private set; }

		Stream IConfigSource.InputStream {
			get { return Stream; }
		}

		Stream IConfigSource.OutputStream {
			get { return Stream; }
		}
	}
}