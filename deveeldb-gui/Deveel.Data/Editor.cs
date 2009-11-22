using System;
using System.IO;
using System.Windows.Forms;

using Deveel.Data.Commands;

using ICSharpCode.TextEditor.Document;

namespace Deveel.Data {
	public partial class Editor : WeifenLuo.WinFormsUI.Docking.DockContent, IEditor, IBrowsableDocument, ITextSearchProvider {
		public Editor(IApplicationServices services) {
			InitializeComponent();
			this.services = services;

			CommandControlBuilder controlBuilder = new CommandControlBuilder(services.CommandHandler);

			contextMenuStrip.Items.Add(controlBuilder.CreateToolStripMenuItem(typeof(SaveFileCommand)));
			contextMenuStrip.Items.Add(new ToolStripSeparator());
			contextMenuStrip.Items.Add(controlBuilder.CreateToolStripMenuItem(typeof(CloseActiveChildCommand)));
			contextMenuStrip.Items.Add(controlBuilder.CreateToolStripMenuItem(typeof(CloseChildrenCommand)));
			contextMenuStrip.Items.Add(controlBuilder.CreateToolStripMenuItem(typeof(CopyQueryEditorFileNameCommand)));

			CommandControlBuilder.MonitorMenuItemsOpeningForEnabling(contextMenuStrip);
		}

		private readonly IApplicationServices services;

		private string fileName;
		private bool highlighterLoaded;
		private bool dirty;
		private ITextSearchService searchService;

		private void Editor_Load(object sender, EventArgs e) {
#if DEBUG
			lblEditorInfo.Text = GetType().FullName;
#else
			lblEditorInfo.Visible = false;
#endif
			textEditor.Document.DocumentChanged += new ICSharpCode.TextEditor.Document.DocumentEventHandler(Document_DocumentChanged);
		}

		void Document_DocumentChanged(object sender, DocumentEventArgs e) {
			dirty = true;
		}

		private void SetTabTextByFilename() {
			string dirtyText = string.Empty;
			string text = "Untitled";
			string tabtext;

			if (dirty)
				dirtyText = " *";

			if (textEditor.FileName != null) {
				text = FileName;
				tabtext = Path.GetFileName(FileName);
			} else {
				text += services.Settings.CountUntitled();
				tabtext = text;
			}

			TabText = tabtext + dirtyText;
			ToolTipText = text + dirtyText;
		}

		public void LoadHighlightingProvider() {
			if (highlighterLoaded)
				return;

			// see: http://wiki.sharpdevelop.net/Syntax%20highlighting.ashx
			string dir = Path.GetDirectoryName(GetType().Assembly.Location);
			FileSyntaxModeProvider fsmProvider = new FileSyntaxModeProvider(dir);
			HighlightingManager.Manager.AddSyntaxModeFileProvider(fsmProvider);
			highlighterLoaded = true;
		}

		#region Implementation of IEditor

		public string FileName {
			get { return fileName; }
			set {
				fileName = value;
				Text = FileName;
				TabText = FileName;
			}
		}

		public string FileFilter {
			get { return "Text Files (*.txt)|*.txt|All Files (*.*)|*.*"; }
		}

		public bool HasChanges {
			get { return dirty; }
			set {
				if (dirty != value) {
					dirty = value;
					SetTabTextByFilename();
				}
			}
		}

		string IEditor.Text {
			get { return textEditor.Text; }
		}

		public bool SupportsHistory {
			get { return true; }
		}

		public string Content {
			get { return textEditor.Text; }
			set { textEditor.Text = value; }
		}

		public string SelectedText {
			get { return textEditor.ActiveTextAreaControl.SelectionManager.SelectedText; }
		}

		public void SetSyntax(string name) {
			LoadHighlightingProvider();
			textEditor.SetHighlighting(name);
		}

		public void LoadFile() {
			textEditor.LoadFile(FileName);
			dirty = false;
		}

		public void SaveFile() {
			textEditor.SaveFile(FileName);
			dirty = false;

		}

		public void Insert(string text) {
			if (text == null || text.Length == 0)
				return;

			int offset = textEditor.ActiveTextAreaControl.Caret.Offset;

			// if some text is selected we want to replace it
			if (textEditor.ActiveTextAreaControl.SelectionManager.IsSelected(offset)) {
				offset = textEditor.ActiveTextAreaControl.SelectionManager.SelectionCollection[0].Offset;
				textEditor.ActiveTextAreaControl.SelectionManager.RemoveSelectedText();
			}

			textEditor.Document.Insert(offset, text);
			int newOffset = offset + text.Length;

			if (CursorOffset != newOffset)
				SetCursorOffset(newOffset);

			textEditor.Focus();
		}

		public void ClearSelectedText() {
			textEditor.ActiveTextAreaControl.SelectionManager.ClearSelection();
		}

		public void HighlightText(int startIndex, int length) {
			if (startIndex < 0 || length < 1)
				return;

			int endPos = startIndex + length;
			textEditor.ActiveTextAreaControl.SelectionManager.SetSelection(
				textEditor.Document.OffsetToPosition(startIndex),
				textEditor.Document.OffsetToPosition(endPos));
			SetCursorOffset(endPos);
		}

		public void Undo() {
			textEditor.Undo();
		}

		public void Redo() {
			textEditor.Redo();
		}

		#endregion

		#region Implementation of ICursorOffsetHandler

		public int CursorOffset {
			get { return textEditor.ActiveTextAreaControl.Caret.Offset; }
		}

		#endregion

		#region Implementation of IBrowsableDocument

		public int Line {
			get { return textEditor.ActiveTextAreaControl.Caret.Line; }
			set { textEditor.ActiveTextAreaControl.Caret.Line = value; }
		}

		public int Column {
			get { return textEditor.ActiveTextAreaControl.Caret.Column; }
			set { textEditor.ActiveTextAreaControl.Caret.Column = value; }
		}

		public int TotalLines {
			get { return textEditor.Document.TotalNumberOfLines; }
		}

		public bool SetCursorOffset(int value) {
			if (value >= 0) {
				textEditor.ActiveTextAreaControl.Caret.Position = textEditor.Document.OffsetToPosition(value);
				return true;
			}

			return false;
		}

		public bool SetCursorPosition(int line, int column) {
			if (line > TotalLines)
				return false;

			textEditor.ActiveTextAreaControl.Caret.Line = line;
			textEditor.ActiveTextAreaControl.Caret.Column = column;

			return true;
		}

		#endregion

		#region Implementation of ITextSearchProvider

		public ITextSearchService TextSearchService {
			get {
				if (searchService == null)
					searchService = (ITextSearchService) services.Container.Resolve(typeof(ITextSearchService));
				return searchService;
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

			textEditor.Document.Replace(offset, length, newValue);

			return true;
		}

		#endregion

		private void Editor_FormClosing(object sender, FormClosingEventArgs e) {
			if (dirty) {
				DialogResult saveFile = services.HostWindow.DisplayMessageBox(
					this,
					"There are changes not saved: do you want to save the file?" + Environment.NewLine + TabText, "Save",
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
	}
}
