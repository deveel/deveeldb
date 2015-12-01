using System;

using Deveel.Data.Util;

namespace Deveel.Data.Protocol {
	/// <summary>
	/// Represents a stream that supports required functionalities
	/// for a <see cref="LengthMarkedBufferedInputStream"/>
	/// </summary>
	public interface IInputStream {
		/// <summary>
		/// Gets ths available bytes to be read on the underlying stream.
		/// </summary>
		int Available { get; }

		int Read(byte[] bytes, int offset, int length);
	}
}