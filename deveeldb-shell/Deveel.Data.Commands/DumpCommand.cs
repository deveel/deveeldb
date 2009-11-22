#if DEBUG
using System;
using System.Collections;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

using Deveel.Commands;
using Deveel.Data.Shell;
using Deveel.Design;
using Deveel.Shell;
using Deveel.Zip;

namespace Deveel.Data.Commands {
	[Command("dump")]
	[CommandSynopsis("dump in <file-name> [ commit every <number> ]")]
	[CommandSynopsis("dump out <file-name>")]
	internal class DumpCommand : Command, IInterruptable {
		private volatile bool _running;

		internal readonly String FileEncoding = "UTF-8";

		internal const int DUMP_VERSION = 1;

		public override bool RequiresContext {
			get { return false; }
		}

		internal bool IsRunning {
			get { return _running; }
			set { _running = value; }
		}

		internal TextWriter OpenOutputStream(string fileName, string encoding) {
			string f = ((DeveelDBShell)Application).OpenFile(fileName);
			Stream outStream = new FileStream(f, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
			if (fileName.EndsWith(".gz"))
				outStream = new GZIPOutputStream(outStream, 4096);
			outStream.Seek(0, SeekOrigin.Begin);
			return new StreamWriter(outStream, Encoding.GetEncoding(encoding));
		}

		internal TextReader OpenInputReader(string fileName, string fileEncoding) {
			string f = ((DeveelDBShell)Application).OpenFile(fileName);
			Stream inStream = new FileStream(f, FileMode.Open, FileAccess.Read, FileShare.Read);
			if (fileName.EndsWith(".gz"))
				inStream = new GZIPInputStream(inStream);
			return new StreamReader(inStream, Encoding.GetEncoding(fileEncoding));
		}

		string lastProblem = null;
		long problemCount = 0;
		internal void ReportProblem(String msg) {
			if (msg == null)
				return;
			if (msg.Equals(lastProblem)) {
				++problemCount;
			} else {
				FinishProblemReports();
				problemCount = 1;
				Application.MessageDevice.Write("Problem: " + msg);
				lastProblem = msg;
			}
		}

		internal void FinishProblemReports() {
			if (problemCount > 1) {
				Application.MessageDevice.Write("   (" + problemCount + " times)");
			}
			if (problemCount > 0)
				Application.MessageDevice.WriteLine();
			lastProblem = null;
			problemCount = 0;
		}

		public override CommandResultCode Execute(object context, string[] args) {
			SqlSession session = (SqlSession) context;

			int argc = args.Length;

			if (argc < 2)
				return CommandResultCode.SyntaxError;

			string opt = args[0];

			if (opt == "in") {
				if (session == null)
					return CommandResultCode.ExecutionFailed;

				string fileName = args[1];
				int commitPoint = -1;

				if (argc > 2) {
					if (argc < 5)
						return CommandResultCode.SyntaxError;
					if (String.Compare(args[2], "commit", true) != 0)
						return CommandResultCode.SyntaxError;
					if (String.Compare(args[3], "every", true) != 0)
						return CommandResultCode.SyntaxError;

					try {
						commitPoint = Int32.Parse(args[4]);
					} catch (FormatException) {
						Application.MessageDevice.WriteLine("commit point number expected");
						return CommandResultCode.SyntaxError;
					}
				}

				DumpImporter importer = new DumpImporter(this, fileName);
				importer.CommitPoint = commitPoint;
				importer.Import(session);
			} else if (opt == "out") {

			} else if (opt == "select") {
				if ((argc < 4))
					return CommandResultCode.SyntaxError;
				string fileName = args[1];
				string tabName = args[2];
				string select = args[3];
				if (!select.ToUpper().Equals("SELECT")) {
					Application.MessageDevice.WriteLine("'select' expected..");
					return CommandResultCode.SyntaxError;
				}
				StringBuilder statement = new StringBuilder("select");
				for (int i = 4; i < argc; i++) {
					statement.Append(" ").Append(args[i]);
				}
			}

			//TODO: continue

			return CommandResultCode.SyntaxError;
		}

		internal void BeginInterruptableSection() {
			_running = true;
			SignalInterruptHandler.Current.Push(this);
		}

		internal void EndInterruptableSection() {
			SignalInterruptHandler.Current.Pop();
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void Interrupt() {
			_running = false;
		}

		public override IEnumerator Complete(CommandDispatcher dispatcher, string partialCommand, string lastWord) {
			string[] st = partialCommand.Split(' ');
			String cmd = st[0];
			int argc = st.Length;
			if (lastWord.Length > 0) {
				argc--;
			}

			string type = st[1];

			if (type.Equals("conditional")) {
				if (argc == 1) {
					return new FileEnumerator(partialCommand, lastWord);
				} else if (argc == 2) {
					if (lastWord.StartsWith("\"")) {
						lastWord = lastWord.Substring(1);
					}
					return ((DeveelDBShell)Application).CurrentSession.CompleteTableName(lastWord);
				} else if (argc > 1) {
					string filename = st[2]; // discard filename.
					string table = st[3];
					ICollection columns = ((DeveelDBShell)Application).CurrentSession.ColumnsFor(table);
					NameCompleter compl = new NameCompleter(columns);
					return compl.GetAlternatives(lastWord);
				}
			} else if (type.Equals("out")) {
				// this is true for dump-out und verify-dump
				if (argc == 1)
					return new FileEnumerator(partialCommand, lastWord);

				if (argc > 1) {
					if (lastWord.StartsWith("\""))
						lastWord = lastWord.Substring(1);

					ArrayList alreadyGiven = new ArrayList();
					/*
					 * do not complete the tables we already gave on the
					 * commandline.
					 */
					for (int i = 2; i < argc; i++) {
						alreadyGiven.Add(st[i]);
					}

					IEnumerator it = ((DeveelDBShell)Application).CurrentSession.CompleteTableName(lastWord);
					return new TableEnumerator(it, alreadyGiven);
				}
			} else {
				if (argc == 0) {
					return new FileEnumerator(partialCommand, lastWord);
				}
			}
			return null;
		}

		private class TableEnumerator : IEnumerator {
			public TableEnumerator(IEnumerator it, ArrayList alreadyGiven) {
				this.it = it;
				this.alreadyGiven = alreadyGiven;
			}

			private readonly IEnumerator it;
			string table = null;
			private readonly ArrayList alreadyGiven;

			public bool MoveNext() {
				while (it.MoveNext()) {
					table = (String)it.Current;
					if (alreadyGiven.Contains(table)) {
						continue;
					}
					return true;
				}
				return false;
			}

			public object Current {
				get { return table; }
			}

			public void Reset() {
				it.Reset();
			}
		}
	}
}
#endif