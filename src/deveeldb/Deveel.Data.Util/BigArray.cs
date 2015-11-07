using System;

namespace Deveel.Data.Util {
	public static class BigArray {
		private static int _defaultBlockLength = 10 * 1024 * 1024;

		public static int DefaultBlockLength {
			get { return _defaultBlockLength; }
			set {
				if (value < 1024)
					throw new ArgumentOutOfRangeException("value", "Minimum DefaultBlockLength value is 1024kb (1024*1024).");

				_defaultBlockLength = value;
			}
		}

		public static void Copy<T>(BigArray<T> source, long sourceIndex, BigArray<T> dest, long destIndex, long count) {
			if (source == null)
				throw new ArgumentNullException("source");

			if (sourceIndex < 0 || sourceIndex >= source.Length)
				throw new ArgumentOutOfRangeException("sourceIndex");

			if (dest == null)
				throw new ArgumentNullException("dest");

			if (destIndex < 0 || destIndex >= dest.Length)
				throw new ArgumentOutOfRangeException("destIndex");

			if (destIndex + count >= dest.Length)
				throw new ArgumentException("Cannot copy over the maximum destination capacity");

			for (var i = 0; i < count; i++) {
				dest[destIndex + i] = source[sourceIndex + i];
			}
		}
	}
}
