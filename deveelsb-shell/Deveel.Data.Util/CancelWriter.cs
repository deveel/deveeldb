using System;

using Deveel.Shell;

namespace Deveel.Data.Util {
	/**
 * Little utility that allows to write a string to the
 * screen and cancel it afterwards (with Backspaces). Will only
 * write, if the Output is indeed a terminal.
 */
	public sealed class CancelWriter {
		private const String BACKSPACE = "\b";

		private readonly IOutputDevice _out;
		private readonly bool _doWrite;

		/** the string that has been written. 'null', if
		 * nothing has been written or it is cancelled 
		 */
		private String _writtenString;

		public CancelWriter(IOutputDevice output) {
			_out = output;
			_doWrite = _out.IsTerminal;
			_writtenString = null;
		}

		/**
		 * returns, wether this cancel writer will print
		 * anything. Depends on the fact, that the output
		 * is a terminal.
		 */
		public bool isPrinting() {
			return _doWrite;
		}

		/**
		 * returns, if this cancel writer has any cancellable
		 * output.
		 */
		public bool hasCancellableOutput() {
			return _writtenString != null;
		}

		/**
		 * Print message to screen. Cancel out any previous
		 * message. If the output is no terminal, do not
		 * write anything.
		 * @param str string to print. Must not be null.
		 */
		public void print(String str) {
			if (!_doWrite) return;
			int charCount = cancel(false);
			_out.Write(str);
			_writtenString = str;

			/* wipe the difference between the previous
			 * message and this one */
			int lenDiff = charCount - str.Length;
			if (lenDiff > 0) {
				writeChars(lenDiff, " ");
				writeChars(lenDiff, BACKSPACE);
			}
			_out.Flush();
		}

		/**
		 * cancel out the written string and wipe it
		 * with spaces.
		 */
		public int cancel() {
			return cancel(true);
		}

		/**
		 * cancel the output.
		 * @param wipeOut 'true', if the written characters 
		 *        should be wiped out with spaces. Otherwise,
		 *        the cursor is placed at the beginning of
		 *        the string without wiping.
		 * @return number of characters cancelled.
		 */
		public int cancel(bool wipeOut) {
			if (_writtenString == null)
				return 0;
			int backspaceCount = _writtenString.Length;
			writeChars(backspaceCount, BACKSPACE);
			if (wipeOut) {
				writeChars(backspaceCount, " ");
				writeChars(backspaceCount, BACKSPACE);
				_out.Flush();
			}
			_writtenString = null;
			return backspaceCount;
		}

		private void writeChars(int count, String str) {
			for (int i = 0; i < count; ++i) {
				_out.Write(str);
			}
		}
	}
}