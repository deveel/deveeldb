using System;
using System.Collections;
using System.Data;
using System.Drawing;
using System.Drawing.Printing;
using System.IO;
using System.Text;
using System.Windows.Forms;

using Deveel.Data.Commands;

using ICSharpCode.TextEditor.Document;

using WeifenLuo.WinFormsUI.Docking;

namespace Deveel.Data {
	public partial class QueryEditor : DockContent, IQueryEditor, IPrintable {
		public QueryEditor(IApplicationServices services) {
			InitializeComponent();

			commandControlBuilder = new CommandControlBuilder(services.CommandHandler);
			this.services = services;
		}

		private readonly IApplicationServices services;
		private ITextSearchService textSearchService;
		private readonly CommandControlBuilder commandControlBuilder;
		private bool changed;
		private bool highlightLoaded;
		private bool busy;
		private string statusText;
		private SqlQueryExecutor sqlExecutor;

		private readonly object syncLock = new object();

		#region Implementation of IEditor

		public string FileName {
			get { return txtQuery.FileName; }
			set {
				txtQuery.FileName = value;
				SetTabTextByFilename();
			}
		}

		public string FileFilter {
			get { return "SQL Files (*.sql)|*.sql|All Files (*.*)|*.*"; }
		}

		public bool HasChanges {
			get { return changed; }
			set {
				if (changed != value) {
					changed = value;
					SetTabTextByFilename();
				}
			}
		}

		string IEditor.Text {
			get { return Content; }
		}

		public bool SupportsHistory {
			get { return true; }
		}

		public string Content {
			get { return txtQuery.Text; }
			set { txtQuery.Text = value; }
		}

		public string SelectedText {
			get { return txtQuery.ActiveTextAreaControl.SelectionManager.SelectedText; }
		}

		public void SetSyntax(string name) {
			LoadHighlightingProvider();
			txtQuery.SetHighlighting(name);
		}

		public void LoadFile() {
			txtQuery.LoadFile(FileName);
			HasChanges = false;
		}

		public void SaveFile() {
			if (FileName == null)
				throw new InvalidOperationException();

			txtQuery.SaveFile(FileName);
			HasChanges = false;
		}

		public void Insert(string text) {
			if (text == null || text.Length == 0)
				return;

			int offset = txtQuery.ActiveTextAreaControl.Caret.Offset;

			if (txtQuery.ActiveTextAreaControl.SelectionManager.IsSelected(offset)) {
				offset = txtQuery.ActiveTextAreaControl.SelectionManager.SelectionCollection[0].Offset;
				txtQuery.ActiveTextAreaControl.SelectionManager.RemoveSelectedText();
			}

			txtQuery.Document.Insert(offset, text);
			int newOffset = offset + text.Length;

			if (CursorOffset != newOffset)
				SetCursorOffset(newOffset);

			txtQuery.Focus();
		}

		public void ClearSelectedText() {
			txtQuery.ActiveTextAreaControl.SelectionManager.ClearSelection();
		}

		public void HighlightText(int startIndex, int length) {
			if (startIndex < 0 || length < 1)
				return;

			int endPos = startIndex + length;
			txtQuery.ActiveTextAreaControl.SelectionManager.SetSelection(
				txtQuery.Document.OffsetToPosition(startIndex),
				txtQuery.Document.OffsetToPosition(endPos));

			SetCursorOffset(endPos);
		}

		public void Undo() {
			txtQuery.Undo();
		}

		public void Redo() {
			txtQuery.Redo();
		}

		#endregion

		#region Implementation of ITask

		public bool IsBusy {
			get { return busy; }
		}

		public void Execute() {
			string text = SelectedText;

			if (text != null && text.Length > 0) {
				ExecuteQuery(text);
			} else {
				ExecuteQuery(Content);
			}
		}

		public void Cancel() {
		}

		#endregion

		#region Implementation of ICursorOffsetHandler

		public int CursorOffset {
			get { return txtQuery.ActiveTextAreaControl.Caret.Offset; }
		}

		#endregion

		#region Implementation of IBrowsableDocument

		public int Line {
			get { return txtQuery.ActiveTextAreaControl.Caret.Line; }
			set { txtQuery.ActiveTextAreaControl.Caret.Line = value; }
		}

		public int Column {
			get { return txtQuery.ActiveTextAreaControl.Caret.Column; }
			set { txtQuery.ActiveTextAreaControl.Caret.Column = value; }
		}

		public int TotalLines {
			get { return txtQuery.Document.TotalNumberOfLines; }
		}

		public bool SetCursorOffset(int value) {
			if (value >= 0) {
				txtQuery.ActiveTextAreaControl.Caret.Position = txtQuery.Document.OffsetToPosition(value);
				return true;
			}

			return false;
		}

		public bool SetCursorPosition(int line, int column) {
			if (line > TotalLines)
				return false;

			txtQuery.ActiveTextAreaControl.Caret.Line = line;
			txtQuery.ActiveTextAreaControl.Caret.Column = column;
			return true;
		}

		#endregion

		#region Implementation of IQueryBatchHandler

		public SqlQueryBatch Batch {
			get { return sqlExecutor == null ? null : sqlExecutor.Batch; }
		}

		#endregion

		#region Implementation of ITextSearchProvider

		public ITextSearchService TextSearchService {
			get {
				if (textSearchService == null)
					textSearchService = (ITextSearchService) services.Container.Resolve(typeof(ITextSearchService));
				return textSearchService;
			}
		}

		public bool SupportsReplace {
			get { return true; }
		}

		public int Search(string value, int offset, bool ignoreCase) {
			if (value == null || value.Length == 0)
				return -1;

			if (offset < 0)
				return -1;

			string text = Content;
			int pos = text.IndexOf(value, offset, (ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal));
			if (pos > -1) {
				ClearSelectedText();
				HighlightText(pos, value.Length);
			}

			return pos;
		}

		public bool Replace(string newValue, int offset, int length) {
			if (newValue == null || offset < 0 || length < 0)
				return false;

			if ((offset + length) > Content.Length)
				return false;

			txtQuery.Document.Replace(offset, length, newValue);
			return true;
		}

		#endregion

		#region Implementation of IQueryEditor

		public System.Windows.Forms.Control Control {
			get { return txtQuery; }
		}

		public void SetStatus(string text) {
			statusText = text;
			UpdateHostStatus();
		}

		#endregion

		#region Implementation of IPrintable

		public PrintDocument PrintDocument {
			get { return txtQuery.PrintDocument; }
		}

		#endregion

		private void QueryEditor_Load(object sender, EventArgs e) {
			LoadHighlightingProvider();
			txtQuery.Document.DocumentChanged += new ICSharpCode.TextEditor.Document.DocumentEventHandler(Document_DocumentChanged);

			contextMenuStrip.Items.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(ExecuteTaskCommand)));
			contextMenuStrip.Items.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(CancelTaskCommand)));

			editorContextMenuStrip.Items.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(SaveFileCommand)));
			editorContextMenuStrip.Items.Add(new ToolStripSeparator());
			editorContextMenuStrip.Items.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(CloseActiveChildCommand)));
			editorContextMenuStrip.Items.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(CloseChildrenCommand)));
			editorContextMenuStrip.Items.Add(commandControlBuilder.CreateToolStripMenuItem(typeof(CopyQueryEditorFileNameCommand)));

			CommandControlBuilder.MonitorMenuItemsOpeningForEnabling(editorContextMenuStrip);
		}

		void Document_DocumentChanged(object sender, ICSharpCode.TextEditor.Document.DocumentEventArgs e) {
			changed = true;
		}

		public void LoadHighlightingProvider() {
			if (highlightLoaded)
				return;

			// see: http://wiki.sharpdevelop.net/Syntax%20highlighting.ashx
			string dir = Path.GetDirectoryName(GetType().Assembly.Location);
			FileSyntaxModeProvider fsmProvider = new FileSyntaxModeProvider(dir);
			HighlightingManager.Manager.AddSyntaxModeFileProvider(fsmProvider); // Attach to the text editor.
			txtQuery.SetHighlighting("SQL");
			highlightLoaded = true;
		}

		private void SetTabTextByFilename() {
			string dirty = String.Empty;
			string text = "Untitled";
			string tabtext;

			if (changed) {
				dirty = " *";
			}

			if (txtQuery.FileName != null) {
				text = FileName;
				tabtext = Path.GetFileName(FileName);
			} else {
				text += services.Settings.CountUntitled();
				tabtext = text;
			}

			TabText = tabtext + dirty;
			ToolTipText = text + dirty;
		}

		public void ExecuteQuery(string sql) {
			if (IsBusy) {
				services.HostWindow.DisplaySimpleMessageBox(this, "Please wait for the current operation to complete.", "Busy");
				return;
			}

			lock (syncLock) {
				busy = true;
			}

			bool enableBatches = (bool) services.Settings.GetProperty(SettingsProperties.EnableBatching);
			sqlExecutor = new SqlQueryExecutor(services.Settings.ConnectionString.ConnectionString);
			UseWaitCursor = true;
			queryBackgroundWorker.RunWorkerAsync(sql);
		}

		protected void UpdateHostStatus() {
			services.HostWindow.SetStatus(this, statusText);
		}

		private void queryBackgroundWorker_DoWork(object sender, System.ComponentModel.DoWorkEventArgs e) {
			string sql = (string)e.Argument;
			sqlExecutor.QueryExecuted += new QueryEventHandler(sqlExecutor_QueryExecuted);
			sqlExecutor.Execute(sql);
		}

		private void queryBackgroundWorker_ProgressChanged(object sender, System.ComponentModel.ProgressChangedEventArgs e) {
			SetStatus(string.Format("Processing batch {0}%...", e.ProgressPercentage));
		}

		private void queryBackgroundWorker_RunWorkerCompleted(object sender, System.ComponentModel.RunWorkerCompletedEventArgs e) {
			try {
				sqlExecutor.QueryExecuted -= new QueryEventHandler(sqlExecutor_QueryExecuted);

				if (e.Error != null) {
					services.HostWindow.DisplaySimpleMessageBox(this, e.Error.Message, "Error");
					SetStatus(e.Error.Message);
				} else {
					services.HostWindow.SetPointerState(Cursors.Default);
					string message = CreateQueryCompleteMessage(sqlExecutor.Batch.StartTime, sqlExecutor.Batch.EndTime);
					if (sqlExecutor.LastError != null)
						message = "ERROR - " + message;

					services.HostWindow.SetStatus(this, message);
					PopulateTables();
					txtQuery.Focus();
				}
			} finally {
				UseWaitCursor = false;
				lock (syncLock) {
					busy = false;
				}
			}
		}

		private static string CreateQueryCompleteMessage(DateTime start, DateTime end) {
			TimeSpan ts = end.Subtract(start);
			return String.Format("Query complete, {0:00}:{1:00}.{2:000}", ts.Minutes, ts.Seconds,ts.Milliseconds);
		}

		private void sqlExecutor_QueryExecuted(object sender, QueryEventArgs args) {
			double i = System.Math.Max(1, args.Index);
			double count =  System.Math.Max(1, args.Count);
			queryBackgroundWorker.ReportProgress(Convert.ToInt32(i / count * 100));
		}

		protected Font CreateDefaultFont() {
			return new Font("Courier New", 8.25F, FontStyle.Regular, GraphicsUnit.Point);
		}

		private void ClearGridsAndTabs() {
			for (int i = resultTabControl.TabPages.Count - 1; i >= 0; i--) {
				TabPage tabPage = resultTabControl.TabPages[i];
				if (tabPage.Controls.Count > 0) {
					tabPage.Controls[0].Dispose(); // dispose grid
				}
				resultTabControl.TabPages.Remove(tabPage);
			}
		}

		private void PopulateTables() {
			ClearGridsAndTabs();

			if (Batch != null) {
				string nullText = (string) services.Settings.GetProperty(SettingsProperties.NullString);
				int counter = 1;
				int queryCount = Batch.QueryCount;
				for (int i = 0; i < queryCount; i++) {
					SqlQuery query = Batch[i];

					DataSet ds = query.Result;
					if (ds != null) {
						foreach (DataTable dt in ds.Tables) {
							DataGridView grid = new DataGridView();
							DataGridViewCellStyle cellStyle = new DataGridViewCellStyle();

							grid.AllowUserToAddRows = false;
							grid.AllowUserToDeleteRows = false;
							grid.Dock = DockStyle.Fill;
							grid.Name = "gridResults_" + counter;
							grid.ReadOnly = true;
							grid.DataSource = dt;
							grid.DataError += new DataGridViewDataErrorEventHandler(grid_DataError);
							grid.DefaultCellStyle = cellStyle;
							cellStyle.NullValue = nullText;
							cellStyle.Font = CreateDefaultFont();
							grid.DataBindingComplete += new DataGridViewBindingCompleteEventHandler(grid_DataBindingComplete);
							grid.Disposed += new EventHandler(grid_Disposed);

							TabPage tabPage = new TabPage();
							tabPage.Controls.Add(grid);
							tabPage.Name = "tabPageResults_" + counter;
							tabPage.Padding = new Padding(3);
							tabPage.Text = String.Format("{0}/Table {1}", ds.DataSetName, counter);
							tabPage.UseVisualStyleBackColor = false;

							resultTabControl.TabPages.Add(tabPage);
							grid.AutoResizeColumns(DataGridViewAutoSizeColumnsMode.DisplayedCells);
							counter++;
						}
					}
				}

				string errorMessages = sqlExecutor.ErrorMessages;
				if (errorMessages != null && errorMessages.Length > 0) {
					RichTextBox rtf = new RichTextBox();
					rtf.Font = CreateDefaultFont();
					rtf.Dock = DockStyle.Fill;
					rtf.ScrollBars = RichTextBoxScrollBars.ForcedBoth;
					rtf.Text = errorMessages;

					TabPage tabPage = new TabPage();
					tabPage.Controls.Add(rtf);
					tabPage.Name = "tabPageResults_Messages";
					tabPage.Padding = new Padding(3);
					tabPage.Dock = DockStyle.Fill;
					tabPage.Text = "Messages";
					tabPage.UseVisualStyleBackColor = false;

					resultTabControl.TabPages.Add(tabPage);
				}
			}
		}

		private void grid_Disposed(object sender, EventArgs e) {
			DataGridView grid = sender as DataGridView;
			if (grid == null)
				return;

			grid.DataBindingComplete -= new DataGridViewBindingCompleteEventHandler(grid_DataBindingComplete);
			grid.Disposed -= new EventHandler(grid_Disposed);

		}

		void grid_DataBindingComplete(object sender, DataGridViewBindingCompleteEventArgs e) {
			DataGridView grid = sender as DataGridView;
			if (grid == null)
				return;

			System.Data.DataTable dt = grid.DataSource as System.Data.DataTable;
			if (dt == null)
				return;

			string nullText = (string) services.Settings.GetProperty(SettingsProperties.NullString);
			string dateTimeFormat = (string)services.Settings.GetProperty(SettingsProperties.DateTimeFormat);

			for (int i = 0; i < dt.Columns.Count; i++) {
				if (dt.Columns[i].DataType == typeof(DateTime)) {
					DataGridViewCellStyle dateCellStyle = new DataGridViewCellStyle();
					dateCellStyle.NullValue = nullText;
					dateCellStyle.Format = dateTimeFormat;
					grid.Columns[i].DefaultCellStyle = dateCellStyle;
				}
			}
		}

		private static void grid_DataError(object sender, DataGridViewDataErrorEventArgs e) {
			e.ThrowException = false;
		}

		private void QueryEditor_Activated(object sender, EventArgs e) {
			UpdateHostStatus();
		}

		private void QueryEditor_Deactivate(object sender, EventArgs e) {
			services.HostWindow.SetStatus(this, String.Empty);
		}

		private void QueryEditor_FormClosing(object sender, FormClosingEventArgs e) {
			if (changed) {
				DialogResult saveFile = services.HostWindow.DisplayMessageBox(
					this,
					"There are some uncommitted changes in the document: do you want to save them?\r\n" + TabText, "Save Changes?",
					MessageBoxButtons.YesNoCancel,
					MessageBoxIcon.Question,
					MessageBoxDefaultButton.Button1,
					0,
					null,
					null);

				if (saveFile == DialogResult.Cancel) {
					e.Cancel = true;
				} else if (saveFile == DialogResult.Yes) {
					services.CommandHandler.GetCommand(typeof(SaveFileCommand)).Execute();
				}
			}
		}

		private void selectAllToolStripMenuItem_Click(object sender, EventArgs e) {
			DataGridView grid = (DataGridView)resultTabControl.SelectedTab.Controls[0];
			grid.SelectAll();
		}

		private void copySelectedToolStripMenuItem_Click(object sender, EventArgs e) {
			CopyDialog dialog = null;

			try {
				DataGridView grid = (DataGridView)resultTabControl.SelectedTab.Controls[0];

				if (grid.SelectedCells.Count == 0) {
					return;
				}

				dialog = new CopyDialog();

				if (dialog.ShowDialog() == DialogResult.Cancel)
					return;

				SortedList headers = new SortedList();
				SortedList rows = new SortedList();

				string delimiter = dialog.Separator;
				StringBuilder line = new StringBuilder();

				for (int i = 0; i < grid.SelectedCells.Count; i++) {
					DataGridViewCell cell = grid.SelectedCells[i];
					DataGridViewColumn col = cell.OwningColumn;

					if (!headers.ContainsKey(col.Index))
						headers.Add(col.Index, col.Name);
					if (!rows.ContainsKey(cell.RowIndex))
						rows.Add(cell.RowIndex, cell.RowIndex);
				}

				if (dialog.IncludeHeaders) {
					for (int i = 0; i < headers.Count; i++) {
						line.Append((string)headers.GetByIndex(i));

						if (i < headers.Count - 1)
							line.Append(delimiter);
					}

					line.AppendLine();
				}

				for (int i = 0; i < rows.Count; i++) {
					DataGridViewRow row = grid.Rows[(int)rows.GetKey(i)];
					DataGridViewCellCollection cells = row.Cells;

					for (int j = 0; j < headers.Count; j++) {
						DataGridViewCell cell = cells[(int)headers.GetKey(j)];

						if (cell.Selected) {
							line.Append(cell.Value);
						}
						if (j < headers.Count - 1) {
							line.Append(delimiter);
						}
					}

					line.AppendLine();
				}

				if (line.Length > 0) {
					Clipboard.Clear();
					Clipboard.SetText(line.ToString());

					services.HostWindow.SetStatus(this, "Selected data has been copied to your clipboard");
				}
			} finally {
				if (dialog != null)
					dialog.Dispose();
			}
		}
	}
}
