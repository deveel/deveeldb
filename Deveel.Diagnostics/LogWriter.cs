//
//  This file is part of DeveelDB.
//
//    DeveelDB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of the 
//    License, or (at your option) any later version.
//
//    DeveelDB is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public 
//    License along with DeveelDB.  If not, see <http://www.gnu.org/licenses/>.
//
//  Authors:
//    Antonello Provenzano <antonello@deveel.com>
//    Tobias Downer <toby@mckoi.com>
//

using System;
using System.IO;
using System.Text;

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

		/**
		 * Constructs the log writer.  The 'base_name' is the name of log file.
		 * 'max_size' is the maximum size the file can grow to before it is
		 * copied to a log archive.
		 */
		///<summary>
		///</summary>
		///<param name="base_name"></param>
		///<param name="max_size"></param>
		///<param name="archive_count"></param>
		///<exception cref="ApplicationException"></exception>
		public LogWriter(string base_name, long max_size, int archive_count) {

			if (archive_count < 1) {
				throw new ApplicationException("'archive_count' must be 1 or greater.");
			}

			this.log_file = base_name;
			this.max_size = max_size;
			this.archive_count = archive_count;

			// Does the file exist?
			if (File.Exists(base_name)) {
				log_file_size = new FileInfo(base_name).Length;
			} else {
				log_file_size = 0;
			}
			output = new StreamWriter(base_name, true);

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
					File.Move(source, dest);
				}
				File.Move(log_file, log_file + ".1");

				// Create the new empty log file,
				output = new StreamWriter(log_file, true);
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
			get { return Encoding.UTF8; }
		}

		/// <inheritdoc/>
		public override void Flush() {
			lock (this) {
				output.Flush();
				CheckLogSize();
			}
		}

		/// <inheritdoc/>
		public override void Close() {
			lock (this) {
				output.Flush();
				output.Close();
			}
		}
	}
}