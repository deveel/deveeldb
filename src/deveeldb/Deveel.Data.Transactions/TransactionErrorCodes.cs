using System;

namespace Deveel.Data.Transactions {
	public static class TransactionErrorCodes {
		public const int TableRemoveClash = 0x0223005;
		public const int RowRemoveClash = 0x00981100;
		public const int TableDropped = 0x01100200;
		public const int DuplicateTable = 0x0300210;
		public const int ReadOnly = 0x00255631;
		public const int DirtySelect = 0x007811920;
	}
}
