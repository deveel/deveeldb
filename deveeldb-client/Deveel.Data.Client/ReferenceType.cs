using System;

namespace Deveel.Data.Client {
	[Flags]
	public enum ReferenceType {
		Binary = 2,
		Ascii = 3,
		Unicode = 4,
		Compressed = 0x010
	}
}