using System;

namespace Deveel.Data.Transactions {
	[Flags]
	public enum AccessType {
		Read = 1,
		Write = 2,
		ReadWrite = Read | Write
	}
}