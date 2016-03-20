using System;
using System.Text;

namespace Deveel.Data.Diagnostics {
	class LogMessage {
		public string Database { get; set; }

		public string User { get; set; }

		public DateTimeOffset TimeStamp { get; set; }

		public int ErrorCode { get; set; }

		public string Text { get; set; }

		public override string ToString() {
			return String.Format("[{0}][{1}] - {2:O} - {3}", Database, User, TimeStamp, Text);
		}
	}
}