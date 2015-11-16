using System;
using System.IO;

namespace Deveel.Data.Util {
	public static class StreamExtensions {
		public static void CopyTo(this Stream source, Stream destination) {
			CopyTo(source, destination, 2048);
		}

		public static void CopyTo(this Stream source, Stream destination, int bufferSize) {
			if (source == null)
				throw new ArgumentNullException("source");
			if (destination == null)
				throw new ArgumentNullException("destination");

			if (!source.CanRead)
				throw new ArgumentException("The source stream cannot be read.");
			if (!destination.CanWrite)
				throw new ArgumentException("The destination stream cannot be write");

			var buffer = new byte[bufferSize];
			int readCount;

			while ((readCount = source.Read(buffer, 0, bufferSize)) != 0) {
				destination.Write(buffer, 0, readCount);
			}
		}
	}
}
