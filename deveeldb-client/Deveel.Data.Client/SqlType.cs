namespace Deveel.Data.Client {
	internal enum SqlType {
		Bit = -7,
		TinyInt = -6,
		SmallInt = 5,
		Integer = 4,
		BigInt = -5,
		Float = 6,
		Real = 7,
		Double = 8,
		Numeric = 2,
		Decimal = 3,
		Char = 1,
		VarChar = 12,
		LongVarChar = -1,
		Date = 91,
		Time = 92,
		TimeStamp = 93,
		Interval = 100,

		Binary = -2,
		VarBinary = -3,
		LongVarBinary = -4,

		Null = 0,

		Object = 2000,

		Blob = 2004,
		Clob = 2005,
		Ref = 2006,
		Boolean = 16,

		QueryPlanNode = -19443,
		Unknown = -9332,
		Identity = 56
	}
}