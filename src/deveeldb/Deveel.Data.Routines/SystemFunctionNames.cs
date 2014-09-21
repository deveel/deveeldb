using System;
using System.Runtime.Remoting.Messaging;

namespace Deveel.Data.Routines {
	public static class SystemFunctionNames {
		// Arithmetic Functions
		public const string Abs = "abs";
		public const string Arc = "arc";
		public const string Sin = "sin";
		public const string ASin = "asin";
		public const string SinH = "sinh";
		public const string Cos = "cos";
		public const string Cot = "cot";
		public const string CosH = "cosh";
		public const string ACos = "acos";
		public const string Tan = "tan";
		public const string TanH = "tanh";
		public const string ATan = "atan";
		public const string Log = "log";
		public const string Log10 = "log10";
		public const string Mod = "mod";
		public const string Round = "round";
		public const string Sqrt = "sqrt";
		public const string Pow = "pow";
		public const string Radians = "radians";
		public const string Degrees = "degrees";
		public const string Exp = "exp";
		public const string Rand = "rand";
		public const string Sign = "sign";
		public const string Floor = "floor";
		public const string Ceil = "ceil";
		public const string E = "e";
		public const string Pi = "pi";

		// String Functions
		public const string Concat = "concat";
		public const string Replace = "replace";
		public const string InStr = "instr";
		public const string Substring = "substring";
		public const string LeftTrim = "ltrim";
		public const string RightTrim = "rtrim";
		public const string Trim = "sql_trim";
		public const string LeftPad = "lpad";
		public const string RightPad = "rpad";
		public const string CharLength = "char_length";
		public const string Soundex = "soundex";
		public const string Upper = "upper";
		public const string Lower = "lower";

		// Misc
		public const string Iif = "iif";
		public const string Coalesce = "coalesce";
		public const string Cast = "sql_cast";
		public const string ToNumber = "tonumber";
		public const string Version = "version";
		public const string NullIf = "nullif";

		// Query Functions
		public const string Exists = "sql_exists";
		public const string Unique = "sql_unique";

		public const string Greatest = "greatest";
		public const string Least = "least";
		
		// Sequence Functions
		public const string CurrVal = "currval";
		public const string NextVal = "nextval";
		public const string SetVal = "setval";
		public const string UniqueKey = "uniquekey";
		public const string Identity = "identity";

		// Date/Time Functions
		public const string Date = "dateob";
		public const string Time = "timeob";
		public const string TimeStamp = "timestampob";
		public const string DateFormat = "dateformat";
		public const string AddMonths = "add_months";
		public const string MonthsBetween = "months_between";
		public const string LastDay = "last_day";
		public const string NextDay = "next_day";
		public const string DbTimeZone = "dbtimezone";
		public const string Extract = "extract";
		public const string Year = "year";
		public const string Month = "month";
		public const string Day = "day";
		public const string Hour = "hour";
		public const string Minute = "minute";
		public const string Second = "second";
		public const string Millis = "millis";
		public const string Interval = "intervalob";

		// Binary Functions
		public const string HexToBinary = "hextobinary";
		public const string BinaryToHex = "binarytohex";
		public const string Crc32 = "crc32";
		public const string Adler32 = "adler32";
		public const string OctetLength = "octet_length";
		public const string Compress = "compress";
		public const string Uncompress = "uncompress";
	}
}
