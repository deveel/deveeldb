using System;

namespace Deveel.Data.Diagnostics {
	public class FileErrorLogger : ErrorLogger {
		public int MaxFileSize { get; set; }

		public int MaxFileCount { get; set; }

		protected override void WriteToLog(string message) {
			// TODO:
			base.WriteToLog(message);
		}
	}
}
