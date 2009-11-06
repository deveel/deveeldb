using System;
using System.Collections;
using System.Data;
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
		internal void reportProblem(String msg) {
			if (msg == null)
				return;
			if (msg.Equals(lastProblem)) {
				++problemCount;
			} else {
				finishProblemReports();
				problemCount = 1;
				Application.MessageDevice.Write("Problem: " + msg);
				lastProblem = msg;
			}
		}

		internal void finishProblemReports() {
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

			if (args.Length < 2)
				return CommandResultCode.SyntaxError;

			string opt = args[0];
			
			if (opt == "in") {
				if (session == null)
					return CommandResultCode.ExecutionFailed;

				string fileName = args[1];
				int commitPoint = -1;

				if (args.Length > 2) {
					if (args.Length < 5)
						return CommandResultCode.SyntaxError;
					if (String.Compare(args[2], "commit", true) != 0)
						return CommandResultCode.SyntaxError;
					if (String.Compare(args[3], "every", true) != 0)
						return CommandResultCode.SyntaxError;

					try {
						commitPoint = Int32.Parse(args[4]);
					} catch(FormatException) {
						Application.MessageDevice.WriteLine("commit point number expected");
						return CommandResultCode.SyntaxError;
					}
				}

				DumpImporter importer = new DumpImporter(this, fileName);
				importer.CommitPoint = commitPoint;
				importer.Import(session);
			} else if (opt == "out") {
				
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
	}
}