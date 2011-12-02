using System;

namespace Deveel.Data.Util {
	internal interface IInputStream {
		int Available { get; }

		int Read(byte[] bytes, int offset, int length);
	}
}