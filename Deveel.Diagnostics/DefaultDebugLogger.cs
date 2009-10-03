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

namespace Deveel.Diagnostics {
	/// <summary>
	/// A default implementation of <see cref="IDebugLogger"/> that logs 
	/// messages to a <see cref="TextWriter"/> object.
	/// </summary>
	/// <remarks>
	/// This implementation allows for filtering of log messages of particular
	/// depth.  So for example, only message above or equal to level Alert are
	/// shown.
	/// </remarks>
	public class DefaultDebugLogger : IDebugLogger {

		/**
		 * Set this to true if all alerts to messages are to be output to System.output.
		 * The purpose of this flag is to aid debugging.
		 */
		private const bool PRINT_ALERT_TO_MESSAGES = false;


		/**
		 * The debug Lock object.
		 */
		private readonly Object debug_lock = new Object();

		/**
		 * The PrintWriter for the system output stream.
		 */
		static readonly TextWriter SYSTEM_OUT = Console.Out;

		/**
		 * The PrintWriter for the system error stream.
		 */
		static readonly TextWriter SYSTEM_ERR = Console.Error;


		/**
		 * This variable specifies the level of debugging information that is
		 * output.  Any debugging output above this level is output.
		 */
		private int debug_level = 0;

		/**
		 * The print stream where the debugging information is output to.
		 */
		private TextWriter output = SYSTEM_ERR;

		/**
		 * The print stream where the error information is output to.
		 */
		private TextWriter err = SYSTEM_ERR;


		/**
		 * Internal method that writes output the given information on the output
		 * stream provided.
		 */
		private static void InternalWrite(TextWriter output,
								  DebugLevel level, String class_string, String message) {
			lock (output) {
				if (level < DebugLevel.Message) {
					output.Write("> ");
					output.Write(class_string);
					output.Write(" ( lvl: ");
					output.Write(level);
					output.Write(" )\n  ");
				} else {
					output.Write("% ");
				}
				output.WriteLine(message);
				output.Flush();
			}
		}

		///<summary>
		/// Sets up the <see cref="TextWriter"/> to which the debug information 
		/// is to be output to.
		///</summary>
		///<param name="output"></param>
		public void SetOutput(TextWriter output) {
			this.output = output;
		}

		///<summary>
		/// Sets the debug level that's to be output to the stream.
		///</summary>
		///<param name="level"></param>
		/// <remarks>
		/// Set to 255 to stop all output to the stream.
		/// </remarks>
		public void SetDebugLevel(int level) {
			debug_level = level;
		}

		/**
		 * Sets up the system so that the debug messenger will intercept event
		 * dispatch errors and output the event to the debug stream.
		 */
		/*
		TODO:
	  public void listenToEventDispatcher() {
		// This is only possible in versions of Java post 1.1
	//#IFDEF(NO_1.1)
		// According to the EventDispatchThread documentation, this is just a
		// temporary hack until a proper API has been defined.
		System.setProperty("sun.awt.exception.handler",
						   "com.mckoi.debug.DispatchNotify");
	//#ENDIF
	  }
		*/

		// ---------- Implemented from IDebugLogger ----------

		public bool IsInterestedIn(DebugLevel level) {
			return (level >= debug_level);
		}

		public void Write(DebugLevel level, object ob, string message) {
			Write(level, ob.GetType().Name, message);
		}

		public void Write(DebugLevel level, Type cla, string message) {
			Write(level, cla.Name, message);
		}

		public void Write(DebugLevel level, string class_string, string message) {
			if (IsInterestedIn(level)) {

				if (level >= DebugLevel.Error && level < DebugLevel.Message) {
					InternalWrite(SYSTEM_ERR, level, class_string, message);
				} else if (PRINT_ALERT_TO_MESSAGES) {
					if (output != SYSTEM_ERR && level >= DebugLevel.Alert) { // && level < Message) {
						InternalWrite(SYSTEM_ERR, level, class_string, message);
					}
				}

				InternalWrite(output, level, class_string, message);
			}

		}

		private void WriteTime() {
			lock (output) {
				output.Write("[ TIME: ");
				output.Write(DateTime.Now.ToString());
				output.WriteLine(" ]");
				output.Flush();
			}
		}

		public void WriteException(Exception e) {
			WriteException(DebugLevel.Error, e);
		}

		public void WriteException(DebugLevel level, Exception e) {
			lock (this) {
				if (level >= DebugLevel.Error) {
					lock (SYSTEM_ERR) {
						SYSTEM_ERR.Write("[Deveel.Data.Debug.Debug - Exception thrown: '");
						SYSTEM_ERR.Write(e.Message);
						SYSTEM_ERR.WriteLine("']");
						SYSTEM_ERR.WriteLine(e.StackTrace);
					}
				}

				if (IsInterestedIn(level)) {
					lock (output) {
						WriteTime();
						output.Write("% ");
						output.WriteLine(e.Message);
						output.WriteLine(e.StackTrace);
						output.Flush();
					}
				}
			}
		}
	}
}