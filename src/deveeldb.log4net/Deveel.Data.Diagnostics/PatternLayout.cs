using System;
using System.IO;

using log4net.Core;
using log4net.Util;

namespace Deveel.Data.Diagnostics {
	public sealed class PatternLayout : log4net.Layout.PatternLayout {
		public PatternLayout() {
			AddConverter(new ConverterInfo {
				Name = "eventInfo",
				Type = typeof(PatternConverter)
			});
		}

		#region PatternConverter

		class PatternConverter : log4net.Util.PatternConverter {
			protected override void Convert(TextWriter writer, object state) {
				var loggingEvent = state as LoggingEvent;

				if (loggingEvent == null) {
					writer.Write(SystemInfo.NullText);
					return;
				}

				var message = loggingEvent.MessageObject as LogMessage;
				if (message == null) {
					writer.Write(SystemInfo.NullText);
					return;
				}

				switch (Option.ToUpperInvariant()) {
					case "TEXT":
						writer.Write(message.Text);
						break;
					case "DATABASE":
					case "DATABASENAME":
					case "DB":
					case "DBNAME":
						writer.Write(message.Database);
						break;
					case "USER":
					case "USERNAME":
						writer.Write(message.User);
						break;
					case "ERRORCODE":
					case "CODE":
						writer.Write(message.ErrorCode);
						break;
					case "TIMESTAMP":
					case "TIME":
						writer.Write(message.TimeStamp.ToString("O"));
						break;
					default:
						writer.Write(SystemInfo.NullText);
						break;
				}
			}
		}

		#endregion
	}
}
