// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.IO;
using System.Text;

using Deveel.Data.Store;

namespace Deveel.Diagnostics {
	/// <summary>
	/// A <see cref="TextWriter"/> implementation that writes information to a 
	/// log file that archives old log entries when it goes above a certain 
	/// size.
	/// </summary>
	public class LogWriter : TextWriter {
		/// <summary>
		/// The log file.
		/// </summary>
		private readonly string log_file;
		/// <summary>
		/// The maximum size of the log before it is archived.
		/// </summary>
		private readonly long max_size;
		/// <summary>
		/// The number of backup archives of log files.
		/// </summary>
		private readonly int archive_count;
		/// <summary>
		/// Current size of the log file.
		/// </summary>
		private long log_file_size;
		private TextWriter output;
		private FileStream outputStream;

		///<summary>
		/// Constructs the log writer.
		///</summary>
		///<param name="base_name">The base name of the log file: when the maximum size
		/// is reached, the name of the file will be constructed with this name.</param>
		///<param name="max_size">The maimum size that a log file can grow before
		/// it is archived.</param>
		///<param name="archive_count">The number of maximum archives to keep.</param>
		///<exception cref="ApplicationException"></exception>
		public LogWriter(string base_name, long max_size, int archive_count) {
			if (archive_count < 1)
				throw new ApplicationException("'archive_count' must be 1 or greater.");

			log_file = base_name;
			this.max_size = max_size;
			this.archive_count = archive_count;

			FileMode mode;

			// Does the file exist?
			if (File.Exists(base_name)) {
				log_file_size = new FileInfo(base_name).Length;
				mode = FileMode.Append;
			} else {
				log_file_size = 0;
				mode = FileMode.CreateNew;
			}

			outputStream = new FileStream(base_name, mode, FileAccess.Write, FileShare.Read);
			output = new StreamWriter(outputStream, Encoding.Default);

		}

		/// <summary>
		/// Checks the size of the file, and if it has reached or exceeded the
		/// maximum limit then archive the log.
		/// </summary>
		private void CheckLogSize() {
			if (log_file_size > max_size) {
				// Flush to the log file,
				output.Flush();
				// Close it,
				output.Close();
				output = null;
				// Delete the top archive,
				string top = log_file + "." + archive_count;
				File.Delete(top);
				// Rename backup archives,
				for (int i = archive_count - 1; i > 0; --i) {
					string source = log_file + "." + i;
					string dest = log_file + "." + (i + 1);
					if (File.Exists(source))
						File.Move(source, dest);
				}
				File.Move(log_file, log_file + ".1");

				// Create the new empty log file,
				outputStream = new FileStream(log_file, FileMode.CreateNew, FileAccess.Write, FileShare.Read);
				output = new StreamWriter(outputStream, Encoding.Default);
				log_file_size = 0;
			}
		}

		/// <inheritdoc/>
		public override void Write(char c) {
			lock (this) {
				output.Write(c);
				++log_file_size;
			}
		}

		/// <inheritdoc/>
		public override void Write(char[] cbuf, int off, int len) {
			lock (this) {
				output.Write(cbuf, off, len);
				log_file_size += len;
			}
		}

		/// <inheritdoc/>
		public override Encoding Encoding {
			get { return output.Encoding; }
		}

		/// <inheritdoc/>
		public override void Flush() {
			lock (this) {
				output.Flush();
				FSync.Sync(outputStream);
				CheckLogSize();
			}
		}

		/// <inheritdoc/>
		public override void Close() {
			lock (this) {
				Flush();
				output.Close();
				if (outputStream != null)
					outputStream.Dispose();
				outputStream = null;
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				Close();
			}
			base.Dispose(disposing);
		}
	}
}