using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// Provides extensions to the <see cref="ISqlParser"/> 
	/// interface instances.
	/// </summary>
	public static class SqlParserExtensions {
		public static SqlParseResult Parse(this ISqlParser parser, Stream inputStream) {
			return Parse(parser, inputStream, Encoding.Unicode);
		}

		public static SqlParseResult Parse(this ISqlParser parser, Stream inputStream, Encoding encoding) {
			if (inputStream == null)
				throw new ArgumentNullException("inputStream");
			if (!inputStream.CanRead)
				throw new ArgumentException("The input stream cannot be read.", "inputStream");

			using (var reader = new StreamReader(inputStream, encoding)) {
				var text = reader.ReadToEnd();
				return parser.Parse(text);
			}
		}
	}
}
