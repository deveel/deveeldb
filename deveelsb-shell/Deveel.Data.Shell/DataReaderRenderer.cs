using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

using Deveel.Data.Client;
using Deveel.Design;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	class ResultSetRenderer : IInterruptable {
		private readonly DeveelDbDataReader reader;
		private readonly System.Data.DataTable meta;
		private readonly TableRenderer table;
		private readonly int columns;
		private readonly int[] showColumns;

		private bool beyondLimit;
		private DateTime firstRowTime;
		private readonly long clobLimit = 8192;
		private readonly int rowLimit;
		private volatile bool running;

		public ResultSetRenderer(DeveelDbDataReader reader, string columnDelimiter, bool enableHeader, bool enableFooter, int limit, IOutputDevice output, int[] show) {
			this.reader = reader;
			beyondLimit = false;
			firstRowTime = DateTime.MinValue;
			showColumns = show;
			rowLimit = limit;
			meta = reader.GetSchemaTable();
			columns = (show != null) ? show.Length : meta.Rows.Count;
			table = new TableRenderer(GetDisplayColumns(meta), output, columnDelimiter, enableHeader, enableFooter);
		}

		public ResultSetRenderer(DeveelDbDataReader rset, String columnDelimiter, bool enableHeader, bool enableFooter, int limit, IOutputDevice output)
			: this(rset, columnDelimiter, enableHeader, enableFooter, limit, output, null) {
		}

		// Interruptable interface.
		[MethodImpl(MethodImplOptions.Synchronized)]
		public void Interrupt() {
			running = false;
		}

		public ColumnDesign[] DisplayColumns {
			get { return table.Columns; }
		}

		private String ReadClob(DeveelDbLob c) {
			if (c == null)
				return null;
			StringBuilder result = new StringBuilder();
			long restLimit = clobLimit;
			try {
				Encoding encoding = (c.Type == ReferenceType.AsciiText ? Encoding.ASCII : Encoding.Unicode);
				TextReader input = new StreamReader(c, encoding);
				char[] buf = new char[4096];
				int r;

				while (restLimit > 0
					   && (r = input.Read(buf, 0, (int)System.Math.Min(buf.Length, restLimit))) > 0) {
					result.Append(buf, 0, r);
					restLimit -= r;
				}
			} catch (Exception e) {
				OutputDevice.Message.WriteLine(e.Message);
			}
			if (restLimit == 0) {
				result.Append("...");
			}
			return result.ToString();
		}

		public int Execute() {
			int rows = 0;

			running = true;
			try {
				while (running && reader.Read()) {
					ColumnValue[] currentRow = new ColumnValue[columns];
					for (int i = 0; i < columns; ++i) {
						int col = (showColumns != null) ? showColumns[i] : i;
						System.Data.DataRow row = meta.Rows[col];

						SQLTypes type = (SQLTypes) row["SqlType"];
						string colString = type == SQLTypes.CLOB ? ReadClob(reader.GetLob(col)) : reader.GetString(col);

						ColumnValue thisCol = new ColumnValue(colString);
						currentRow[i] = thisCol;
					}

					if (firstRowTime == DateTime.MinValue)
						// read first row completely.
						firstRowTime = DateTime.Now;

					table.AddRow(currentRow);

					++rows;

					if (rows >= rowLimit) {
						beyondLimit = true;
						break;
					}
				}

				table.CloseTable();

				if (!running) {
					try {
						reader.Command.Cancel();
					} catch (Exception e) {
						OutputDevice.Message.WriteLine("cancel statement failed: " + e.Message);
					}
				}
			} finally {
				reader.Close();
			}
			return rows;
		}

		public bool LimitReached {
			get { return beyondLimit; }
		}

		public DateTime FirstRowTime {
			get { return firstRowTime; }
		}

		// determine meta data necesary for display.
		private ColumnDesign[] GetDisplayColumns(System.Data.DataTable m) {
			ColumnDesign[] result = new ColumnDesign[columns];

			for (int i = 0; i < result.Length; ++i) {
				int col = (showColumns != null) ? showColumns[i] : i;
				ColumnAlignment alignment = ColumnAlignment.Left;
				System.Data.DataRow column = m.Rows[col];
				String columnLabel = column["Name"].ToString();
				SQLTypes type = (SQLTypes) column["SqlType"];

				switch (type) {
					case SQLTypes.NUMERIC:
					case SQLTypes.INTEGER:
					case SQLTypes.REAL:
					case SQLTypes.SMALLINT:
					case SQLTypes.TINYINT:
						alignment = ColumnAlignment.Right;
						break;
				}
				result[i] = new ColumnDesign(columnLabel, alignment);
			}
			return result;
		}
	}
}