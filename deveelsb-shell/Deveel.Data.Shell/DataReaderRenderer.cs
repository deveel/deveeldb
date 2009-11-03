using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

using Deveel.Data.Client;
using Deveel.Design;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	public class ResultSetRenderer : IInterruptable {
		private readonly DeveelDbDataReader rset;
		private readonly System.Data.DataTable meta;
		private readonly TableRenderer table;
		private readonly int columns;
		private readonly int[] showColumns;

		private bool beyondLimit;
		private DateTime firstRowTime;
		private readonly long clobLimit = 8192;
		private readonly int rowLimit;
		private volatile bool running;

		public ResultSetRenderer(DeveelDbDataReader rset,
								 String columnDelimiter,
								 bool enableHeader, bool enableFooter,
								 int limit,
								 IOutputDevice output, int[] show) {
			this.rset = rset;
			beyondLimit = false;
			firstRowTime = DateTime.MinValue;
			showColumns = show;
			rowLimit = limit;
			meta = rset.GetSchemaTable();
			columns = (show != null) ? show.Length : meta.Columns.Count;
			table = new TableRenderer(getDisplayMeta(meta), output,
										  columnDelimiter, enableHeader, enableFooter);
		}

		public ResultSetRenderer(DeveelDbDataReader rset, String columnDelimiter, bool enableHeader, bool enableFooter, int limit, IOutputDevice output)
			: this(rset, columnDelimiter, enableHeader, enableFooter, limit, output, null) {
		}

		// Interruptable interface.
		[MethodImpl(MethodImplOptions.Synchronized)]
		public void Interrupt() {
			running = false;
		}

		public ColumnDesign[] getDisplayMetaData() {
			return table.Columns;
		}

		private String readClob(DeveelDbLob c) {
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

		public int execute() {
			int rows = 0;

			running = true;
			try {
				while (running && rset.Read()) {
					ColumnValue[] currentRow = new ColumnValue[columns];
					for (int i = 0; i < columns; ++i) {
						int col = (showColumns != null) ? showColumns[i] : i + 1;
						System.Data.DataRow row = meta.Rows[col];
						SQLTypes type = (SQLTypes) row["SqlType"];
						String colString;
						if (type == SQLTypes.CLOB) {
							colString = readClob(rset.GetLob(col));
						} else {
							colString = rset.GetString(col);
						}
						ColumnValue thisCol = new ColumnValue(colString);
						currentRow[i] = thisCol;
					}
					if (firstRowTime == DateTime.MinValue) {
						// read first row completely.
						firstRowTime = DateTime.Now;
					}
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
						rset.Command.Cancel();
					} catch (Exception e) {
						OutputDevice.Message.WriteLine("cancel statement failed: " + e.Message);
					}
				}
			} finally {
				rset.Close();
			}
			return rows;
		}

		public bool limitReached() {
			return beyondLimit;
		}

		public DateTime getFirstRowTime() {
			return firstRowTime;
		}

		/**
		 * determine meta data necesary for display.
		 */
		private ColumnDesign[] getDisplayMeta(System.Data.DataTable m) {
			ColumnDesign[] result = new ColumnDesign[columns];

			for (int i = 0; i < result.Length; ++i) {
				int col = (showColumns != null) ? showColumns[i] : i + 1;
				ColumnAlignment alignment = ColumnAlignment.Left;
				System.Data.DataRow column = m.Rows[col];
				String columnLabel = column["Name"].ToString();
				SQLTypes type = (SQLTypes) column["SqlType"];
				/*
				int width = Math.max(m.getColumnDisplaySize(i),
						 columnLabel.length());
				*/
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