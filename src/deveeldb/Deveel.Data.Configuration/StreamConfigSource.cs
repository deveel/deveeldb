// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.IO;

namespace Deveel.Data.Configuration {
	/// <summary>
	/// An implementation of <see cref="IConfigSource"/>
	/// that handles a single <see cref="Stream"/> as
	/// source and destination of the configurations.
	/// </summary>
	public class StreamConfigSource : IConfigSource {
		/// <summary>
		/// Constructs the source with the given stream.
		/// </summary>
		/// <param name="stream">The stream that will be used to read
		/// from or write to.</param>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="stream"/> is <c>null</c>.
		/// </exception>
		public StreamConfigSource(Stream stream) {
			if (stream == null)
				throw new ArgumentNullException("stream");

			Stream = stream;
		}

		/// <summary>
		/// Gets the stream handled by this source.
		/// </summary>
		public Stream Stream { get; private set; }

		Stream IConfigSource.InputStream {
			get { return Stream; }
		}

		Stream IConfigSource.OutputStream {
			get { return Stream; }
		}
	}
}