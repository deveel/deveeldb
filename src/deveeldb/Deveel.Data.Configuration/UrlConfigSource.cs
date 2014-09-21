// 
//  Copyright 2010-2014 Deveel
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
using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;

namespace Deveel.Data.Configuration {
	public sealed class UrlConfigSource : IConfigSource {
		public UrlConfigSource(Uri url) {
			Url = url;
		}

		public UrlConfigSource(string url)
			: this(new Uri(url)) {
		}

		public Uri Url { get; private set; }

		public Stream InputStream {
			get {
				try {
					using (var client = new WebClient()) {
						var data = client.DownloadData(Url);
						return new MemoryStream(data, false);
					}
				} catch (Exception ex) {
					throw new DatabaseConfigurationException(String.Format("Could not load data from the url '{0}'.", Url), ex);
				}
			}
		}

		public Stream OutputStream {
			get { return new WebOutputStream(Url); }
		}

		#region WebOutputStream

		class WebOutputStream : Stream {
			private readonly Uri url;
			private readonly MemoryStream baseStream;
			private bool closed;

			public WebOutputStream(Uri url) {
				this.url = url;
				baseStream = new MemoryStream(1024);
			}

			private void AssertNotClosed() {
				if (closed)
					throw new IOException("The stream is closed and cannot be written.");
			}

			public override void Flush() {
				AssertNotClosed();

				lock (baseStream) {
					baseStream.Flush();

					var data = baseStream.ToArray();
					using (var client = new WebClient()) {
						client.UploadData(url, data);
					}
				}
			}

			public override long Seek(long offset, SeekOrigin origin) {
				AssertNotClosed();

				lock (baseStream) {
					return baseStream.Seek(offset, origin);
				}
			}

			public override void SetLength(long value) {
				AssertNotClosed();

				lock (baseStream) {
					baseStream.SetLength(value);
				}
			}

			public override int Read(byte[] buffer, int offset, int count) {
				throw new NotSupportedException();
			}

			public override void Write(byte[] buffer, int offset, int count) {
				AssertNotClosed();

				lock (baseStream) {
					baseStream.Write(buffer, offset, count);
				}
			}

			public override bool CanRead {
				get { return false; }
			}

			public override bool CanSeek {
				get { return true; }
			}

			public override bool CanWrite {
				get { return false; }
			}

			public override long Length {
				get {
					lock (baseStream) {
						return baseStream.Length;
					}
				}
			}

			public override long Position {
				get {
					lock (baseStream) {
						return baseStream.Position;
					}
				}
				set {
					AssertNotClosed();
					lock (baseStream) {
						baseStream.Position = value;
					}
				}
			}

			public override void Close() {
				try {
					baseStream.Close();
					base.Close();
				} finally {
					closed = true;
				}
			}
		}

		#endregion
	}
}